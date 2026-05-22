// Package producer 提供生产者 HTTP 客户端，供其他服务导入使用。
//
// 生产者将记录缓冲在本地，定时通过 HTTP 批量发送给消费者服务。
// 不依赖 MongoDB，仅依赖标准库 net/http 和 encoding/json。
//
// 特性：
//   - 本地缓冲：最多缓存 QueueSize 条记录
//   - 批量发送：满足条数/字节数/时间任一条件即发送
//   - 自动重试：失败自动重试最多 3 次，递增退避
//   - 鉴权：自动携带 X-Auth-Token 请求头
//   - 空字段校验：Collection/Ops/PSid 为空时丢弃
//   - 并发安全：Send 可在多个 goroutine 中调用
//
// 用法：
//
//	client := producer.New(producer.Config{
//	    ConsumerURL: "http://127.0.0.1:803/bulkwriter/ingest",
//	})
//	defer client.Close()
//
//	client.Send(producer.Record{
//	    Collection: "logs",
//	    Ops:        "bet",
//	    PSid:       "session_123",
//	    Tba:        100.0,
//	    Tid:        "txn_001",
//	    Twla:       95.0,
//	    Gd:         `{"gid":126,"cc":"VND"}`,
//	    CreatedAt:  time.Now().UnixMilli(),
//	})
package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/acoderup/mongo-bulkwriter/model"
)

// Record 是生产者发送的单条记录（model.Record 的类型别名，方便导入）。
type Record = model.Record

// Config 生产者客户端配置。
//
// ConsumerURL 为必填项。其他字段为零值时使用默认值。
type Config struct {
	ConsumerURL   string        // 消费者接收地址，如 http://127.0.0.1:803/bulkwriter/ingest
	AuthToken     string        // 鉴权令牌，需与消费者配置一致（空则不发送）
	FlushInterval time.Duration // 批量发送间隔，默认 100ms
	BatchSize     int           // 批量大小（条数），默认 500
	MaxBatchBytes int           // 批量最大字节数（估算），超过立即发送，默认 8MB
	QueueSize     int           // 本地缓冲队列大小，默认 100000
}

// DefaultConfig 返回推荐配置，只需传入 ConsumerURL。
func DefaultConfig(consumerURL string) Config {
	return Config{
		ConsumerURL:   consumerURL,
		FlushInterval: 100 * time.Millisecond,
		BatchSize:     500,
		MaxBatchBytes: 8 << 20, // 8MB
		QueueSize:     100000,
	}
}

// Client 是生产者客户端，负责缓冲记录并批量发送给消费者。
//
// 内部维护一个缓冲 channel 和一个后台 flusher goroutine。
// 并发安全：Send 可在多个 goroutine 中同时调用。
type Client struct {
	cfg    Config
	queue  chan Record  // 本地缓冲队列
	client *http.Client // HTTP 客户端（5s 超时）
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// New 创建生产者客户端并启动后台发送 goroutine。
//
// 自动填充默认配置（零值字段），创建缓冲队列，启动 flusher goroutine。
func New(cfg Config) *Client {
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 500
	}
	if cfg.MaxBatchBytes == 0 {
		cfg.MaxBatchBytes = 8 << 20 // 8MB
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 100000
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		cfg:    cfg,
		queue:  make(chan Record, cfg.QueueSize),
		client: &http.Client{Timeout: 5 * time.Second},
		cancel: cancel,
	}

	c.wg.Add(1)
	go c.flusher(ctx)

	log.Printf("[producer] started: url=%s batch=%d interval=%v queue=%d",
		cfg.ConsumerURL, cfg.BatchSize, cfg.FlushInterval, cfg.QueueSize)
	return c
}

// Send 非阻塞写入一条记录到本地缓冲队列。
//
// 校验规则：
//   - Collection、Ops、PSid 为空时丢弃并打印错误日志，返回 false
//   - 队列满时丢弃并返回 false
//
// 并发安全，可在多个 goroutine 中同时调用。
func (c *Client) Send(record Record) bool {
	if record.Collection == "" || record.Ops == "" || record.PSid == "" {
		log.Printf("[producer] invalid record dropped: collection=%q ops=%q psid=%q",
			record.Collection, record.Ops, record.PSid)
		return false
	}
	select {
	case c.queue <- record:
		return true
	default:
		return false
	}
}

// Close 关闭生产者，等待最后一批数据发送完成后退出。
//
// 步骤：
//  1. 取消 context，通知 flusher 停止接收
//  2. flusher 刷出剩余数据
//  3. 等待 flusher goroutine 退出
func (c *Client) Close() {
	c.cancel()
	c.wg.Wait()
	log.Println("[producer] closed")
}

// flusher 后台 goroutine：定时从队列取数据，批量 HTTP POST 给消费者。
//
// 发送触发条件（任一满足即立即发送）：
//  1. 达到 BatchSize 条
//  2. 超过 MaxBatchBytes 字节（基于 Gd 字段长度估算）
//  3. 超过 FlushInterval 时间
func (c *Client) flusher(ctx context.Context) {
	defer c.wg.Done()

	batch := make([]Record, 0, c.cfg.BatchSize)
	var byteSize int
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := c.sendBatch(ctx, batch); err != nil {
			log.Printf("[producer] send error: %v", err)
		}
		batch = batch[:0]
		byteSize = 0
	}

	for {
		select {
		case <-ctx.Done():
			// 关闭时用独立 context，避免因 ctx 已取消而丢失最后一批
			if len(batch) > 0 {
				finalCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				c.sendBatch(finalCtx, batch)
				cancel()
			}
			return
		case <-ticker.C:
			flush() // 定时刷新
		case record, ok := <-c.queue:
			if !ok {
				// channel 关闭时同样用独立 context
				if len(batch) > 0 {
					finalCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					c.sendBatch(finalCtx, batch)
					cancel()
				}
				return
			}
			batch = append(batch, record)
			byteSize += len(record.Gd) + 200 // Gd 字符串长度 + 其他字段估算开销
			if len(batch) >= c.cfg.BatchSize || byteSize >= c.cfg.MaxBatchBytes {
				flush()
			}
		}
	}
}

// sendBatch 将一批记录通过 HTTP POST 发送给消费者，失败自动重试最多 3 次。
//
// 重试策略：递增退避 100ms → 200ms → 300ms。
// 自动携带 X-Auth-Token 鉴权头（如已配置）。
func (c *Client) sendBatch(ctx context.Context, batch []Record) error {
	body, err := json.Marshal(model.IngestRequest{Records: batch})
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.ConsumerURL, bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if c.cfg.AuthToken != "" {
			req.Header.Set("X-Auth-Token", c.cfg.AuthToken)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
			return nil
		}
		// 429 或其他错误码，重试
		lastErr = fmt.Errorf("consumer returned %d (attempt %d/3)", resp.StatusCode, attempt+1)
		log.Printf("[producer] %v", lastErr)
	}
	return lastErr
}
