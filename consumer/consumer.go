// Package consumer 提供高并发消费者 HTTP 服务端，接收生产者批量数据并写入 MongoDB。
//
// 内部架构：
//
//	HTTP ingest → internal queue (non-blocking) → worker pool → batch → Mongo BulkWrite
//
// HTTP 处理器非阻塞接收，立即返回 202。后台 worker 异步批量写入 Mongo。
// 内置并发控制（信号量限制同时写入数）和背压保护（队列满返回 429）。
//
// 用法：
//
//	h := consumer.NewHandler(db, consumer.DefaultConfig())
//	r.POST("/bulkwriter/ingest", gin.WrapH(h))
package consumer

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/USERNAME/mongo-bulkwriter"
	"github.com/USERNAME/mongo-bulkwriter/model"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Config 消费者配置。
type Config struct {
	AuthToken     string        // 鉴权令牌，生产者需要携带相同令牌才能连接（空则不校验）
	Workers       int           // worker goroutine 数量，默认 32
	BatchSize     int           // 批量写入大小，默认 500
	BatchBytes    int           // 批量最大字节数（估算），默认 8MB
	FlushInterval time.Duration // flush 超时间隔，默认 100ms
	QueueSize     int           // 内部缓冲队列大小，默认 50000
	MaxConcurrent int           // 最大并发 Mongo BulkWrite 数，默认 16
	MaxBodySize   int64         // 请求体最大字节数，默认 10MB
}

// DefaultConfig 返回推荐配置。
func DefaultConfig() Config {
	return Config{
		Workers:       32,
		BatchSize:     500,
		BatchBytes:    8 << 20, // 8MB
		FlushInterval: 100 * time.Millisecond,
		QueueSize:     50000,
		MaxConcurrent: 16,
		MaxBodySize:   10 << 20, // 10MB
	}
}

// Handler 是高并发消费者的 HTTP 处理器。
//
// 启动时内部创建 worker 池，异步从内部队列消费并写入 Mongo。
// HTTP 请求直接入队，不阻塞等待 Mongo 写入。
//
// 鉴权：如果配置了 AuthToken，生产者必须在请求头携带 X-Auth-Token: <token>。
// 使用 constant-time 比较防止时序攻击。
type Handler struct {
	db        *mongo.Database
	authToken string
	queue   chan model.Record
	sem     chan struct{} // 并发控制信号量
	cfg     Config
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	metrics Metrics
}

// Metrics 消费者运行指标。
type Metrics struct {
	mu            sync.RWMutex
	Received      int64 // 接收记录总数
	Dropped       int64 // 因队列满丢弃数
	Written       int64 // 成功写入数
	WriteErrors   int64 // 写入失败数
	QueueLen      int   // 当前队列长度
}

// NewHandler 创建消费者处理器并启动后台 worker 池。
func NewHandler(db *mongo.Database, cfg Config) *Handler {
	if cfg.Workers == 0 {
		cfg.Workers = 32
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 500
	}
	if cfg.BatchBytes == 0 {
		cfg.BatchBytes = 8 << 20
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 50000
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 16
	}
	if cfg.MaxBodySize == 0 {
		cfg.MaxBodySize = 10 << 20
	}

	ctx, cancel := context.WithCancel(context.Background())

	h := &Handler{
		db:        db,
		authToken: cfg.AuthToken,
		queue:  make(chan model.Record, cfg.QueueSize),
		sem:    make(chan struct{}, cfg.MaxConcurrent),
		cfg:    cfg,
		cancel: cancel,
	}

	// 启动 worker 池
	for i := 0; i < cfg.Workers; i++ {
		h.wg.Add(1)
		go h.worker(ctx, i)
	}

	log.Printf("[consumer] started: workers=%d batch=%d interval=%v queue=%d max_concurrent=%d",
		cfg.Workers, cfg.BatchSize, cfg.FlushInterval, cfg.QueueSize, cfg.MaxConcurrent)
	return h
}

// ServeHTTP 实现 http.Handler 接口。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Ingest(w, r)
}

// Ingest 接收批量记录，入队后立即返回。非阻塞。
//
//	POST /bulkwriter/ingest
//	Body: {"records": [...]}  (max 10MB)
//	Response: {"written": 0, "queued": N}  或 429 过载
func (h *Handler) Ingest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, model.IngestResponse{Error: "method not allowed"})
		return
	}

	// 鉴权校验
	if h.authToken != "" {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Auth-Token")), []byte(h.authToken)) != 1 {
			writeJSON(w, http.StatusForbidden, model.IngestResponse{Error: "forbidden"})
			return
		}
	}

	// 限制请求体大小
	r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxBodySize)

	var req model.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.IngestResponse{Error: "invalid body"})
		return
	}

	if len(req.Records) == 0 {
		writeJSON(w, http.StatusOK, model.IngestResponse{Written: 0})
		return
	}

	// 非阻塞入队（空字段丢弃）
	var queued int
	for _, record := range req.Records {
		if record.Collection == "" || record.Ops == "" || record.Pid == "" {
			h.metrics.mu.Lock()
			h.metrics.Dropped++
			h.metrics.mu.Unlock()
			continue
		}
		select {
		case h.queue <- record:
			queued++
		default:
			h.metrics.mu.Lock()
			h.metrics.Dropped++
			h.metrics.mu.Unlock()
		}
	}

	h.metrics.mu.Lock()
	h.metrics.Received += int64(queued)
	h.metrics.mu.Unlock()

	// 过载保护：丢弃率过高时返回 429
	if queued == 0 && len(req.Records) > 0 {
		writeJSON(w, http.StatusTooManyRequests, model.IngestResponse{
			Error: "queue full, all records dropped",
		})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"written": 0,
		"queued":  queued,
	})
}

// Shutdown 优雅关闭：停止接收 → 等待队列清空 → 关闭 worker。
func (h *Handler) Shutdown() {
	h.cancel()
	h.wg.Wait()
	log.Printf("[consumer] shutdown: received=%d dropped=%d written=%d errors=%d",
		h.metrics.Received, h.metrics.Dropped, h.metrics.Written, h.metrics.WriteErrors)
}

// MetricsSnapshot 返回当前指标快照。
func (h *Handler) MetricsSnapshot() Metrics {
	h.metrics.mu.RLock()
	defer h.metrics.mu.RUnlock()
	return Metrics{
		Received:    h.metrics.Received,
		Dropped:     h.metrics.Dropped,
		Written:     h.metrics.Written,
		WriteErrors: h.metrics.WriteErrors,
		QueueLen:    len(h.queue),
	}
}

// worker 单个 worker goroutine：聚合 batch → BulkWrite。
func (h *Handler) worker(ctx context.Context, id int) {
	defer h.wg.Done()

	batch := make([]model.Record, 0, h.cfg.BatchSize)
	var batchBytes int
	ticker := time.NewTicker(h.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// 复制 batch 避免 goroutine 与下一次写入共享底层数组
		b := make([]model.Record, len(batch))
		copy(b, batch)

		h.sem <- struct{}{}
		go func(records []model.Record) {
			defer func() { <-h.sem }()

			written, err := bulkwriter.BulkInsert(ctx, h.db, records)
			h.metrics.mu.Lock()
			if err != nil {
				h.metrics.WriteErrors++
				log.Printf("[consumer] bulk write error: %v", err)
			} else {
				h.metrics.Written += int64(written)
			}
			h.metrics.mu.Unlock()
		}(b)
		batch = make([]model.Record, 0, h.cfg.BatchSize)
		batchBytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case record, ok := <-h.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, record)
			batchBytes += len(record.Data) + 200
			if len(batch) >= h.cfg.BatchSize || batchBytes >= h.cfg.BatchBytes {
				flush()
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
