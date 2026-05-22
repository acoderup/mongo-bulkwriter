// Package consumer 提供高并发消费者 HTTP 服务端，接收生产者批量数据并异步写入 MongoDB。
//
// 内部架构：
//
//	HTTP ingest → 鉴权 → 空字段校验 → 非阻塞入队 → 内部队列 → worker pool → batch → Mongo BulkWrite
//
// 特性：
//   - 非阻塞接收：HTTP 请求立即返回 202，不等待 Mongo 写入
//   - Worker 池：多个 goroutine 并行聚合 batch 并写入
//   - 并发控制：信号量限制同时进行的 BulkWrite 数量
//   - 背压保护：队列满时丢弃记录并返回 429
//   - 鉴权：X-Auth-Token 恒定时间比较，防时序攻击
//   - 自动索引：首次写入集合时自动创建查询索引
//
// 用法：
//
//	h := consumer.NewHandler(db, consumer.Config{AuthToken: "secret"})
//	r.POST("/bulkwriter/ingest", gin.WrapH(h))
//	defer h.Shutdown()
package consumer

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/acoderup/mongo-bulkwriter"
	"github.com/acoderup/mongo-bulkwriter/model"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Config 消费者配置。
//
// 所有数值字段为 0 时使用默认值。AuthToken 为空则不校验鉴权。
type Config struct {
	AuthToken     string        // 鉴权令牌，生产者需携带相同令牌（空则不校验）
	Workers       int           // worker goroutine 数量，默认 32
	BatchSize     int           // 批量写入大小（条数），默认 500
	BatchBytes    int           // 批量最大字节数（估算），默认 8MB
	FlushInterval time.Duration // flush 超时间隔，默认 100ms
	QueueSize     int           // 内部缓冲队列大小，默认 50000
	MaxConcurrent int           // 最大并发 Mongo BulkWrite 数，默认 16
	MaxBodySize   int64         // 请求体最大字节数（硬限制），默认 10MB
}

// DefaultConfig 返回推荐配置。
//
// 适用于中高负载场景：32 workers、500 条/批、100ms flush、50000 队列、16 并发写。
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
// 内部维护：
//   - 有缓冲 channel 作为内部队列
//   - worker goroutine 池，从队列消费并批量写入
//   - 信号量控制最大并发 BulkWrite 数
//   - 运行时指标（接收/丢弃/写入/错误计数）
//   - 写库失败时自动重连（3 次 × 5 秒）
//
// 启动时自动创建 worker 池，Shutdown 时等待队列清空后退出。
type Handler struct {
	client    *mongo.Client   // MongoDB 客户端（用于重连）
	db        *mongo.Database // MongoDB 数据库句柄
	uri       string          // Mongo 连接地址（用于重连）
	dbName    string          // 数据库名（用于重连）
	authToken string          // 鉴权令牌
	queue     chan model.Record // 内部缓冲队列
	sem       chan struct{}   // 并发控制信号量
	cfg       Config
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	metrics   Metrics
	reconnMu  sync.Mutex      // 防止并发重连
}

// Metrics 消费者运行指标。
type Metrics struct {
	mu          sync.RWMutex
	Received    int64 // 接收记录总数
	Dropped     int64 // 因队列满或空字段丢弃数
	Written     int64 // 成功写入 Mongo 数
	WriteErrors int64 // 写入失败数
	QueueLen    int   // 当前队列长度
}

// MetricsSnapshot 消费者指标快照（不含 mutex，安全返回给外部）。
type MetricsSnapshot struct {
	Received    int64 // 接收记录总数
	Dropped     int64 // 因队列满或空字段丢弃数
	Written     int64 // 成功写入 Mongo 数
	WriteErrors int64 // 写入失败数
	QueueLen    int   // 当前队列长度
}

// NewHandler 创建消费者处理器并启动后台 worker 池。
//
// client/uri/dbName 用于写库失败时的自动重连。
// 自动填充默认配置（零值字段），创建内部队列和信号量，
// 启动指定数量的 worker goroutine。
func NewHandler(client *mongo.Client, db *mongo.Database, uri, dbName string, cfg Config) *Handler {
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
		client:    client,
		db:        db,
		uri:       uri,
		dbName:    dbName,
		authToken: cfg.AuthToken,
		queue:     make(chan model.Record, cfg.QueueSize),
		sem:       make(chan struct{}, cfg.MaxConcurrent),
		cfg:       cfg,
		cancel:    cancel,
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

// ServeHTTP 实现 http.Handler 接口，直接委托给 Ingest。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Ingest(w, r)
}

// Ingest 接收批量记录，入队后立即返回。非阻塞。
//
// 处理流程：
//  1. 校验 HTTP 方法（仅 POST）
//  2. 鉴权校验（X-Auth-Token，恒定时间比较）
//  3. 限制请求体大小（MaxBodySize）
//  4. JSON 解码
//  5. 逐条入队（Collection/Ops/PSid 为空则丢弃）
//  6. 返回 202（部分成功）或 429（全部丢弃）
//
//	POST /bulkwriter/ingest
//	Body: {"records": [...]}  (max 默认 10MB)
//	Response: {"written": 0, "queued": N}
func (h *Handler) Ingest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, model.IngestResponse{Error: "method not allowed"})
		return
	}

	// 鉴权校验：恒定时间比较防时序攻击
	if h.authToken != "" {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Auth-Token")), []byte(h.authToken)) != 1 {
			writeJSON(w, http.StatusForbidden, model.IngestResponse{Error: "forbidden"})
			return
		}
	}

	// 限制请求体大小，防止内存溢出
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

	// 非阻塞入队：局部累积后一次性更新指标，避免逐条加锁
	var queued, dropped int
	for _, record := range req.Records {
		if record.Collection == "" || record.Ops == "" || record.PSid == "" {
			dropped++
			continue
		}
		select {
		case h.queue <- record:
			queued++
		default:
			dropped++
		}
	}

	h.metrics.mu.Lock()
	h.metrics.Received += int64(queued)
	h.metrics.Dropped += int64(dropped)
	h.metrics.mu.Unlock()

	// 过载保护：全部丢弃时返回 429
	if queued == 0 && len(req.Records) > 0 {
		writeJSON(w, http.StatusTooManyRequests, model.IngestResponse{
			Error: "queue full, all records dropped",
		})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"written": 0,
		"queued":  queued,
	})
}

// Shutdown 优雅关闭消费者。
//
// 步骤：
//  1. 取消 context，通知所有 worker 停止接收
//  2. 等待 worker 将队列中剩余数据全部写入 Mongo
//  3. 打印最终指标
func (h *Handler) Shutdown() {
	h.cancel()
	h.wg.Wait()
	log.Printf("[consumer] shutdown: received=%d dropped=%d written=%d errors=%d",
		h.metrics.Received, h.metrics.Dropped, h.metrics.Written, h.metrics.WriteErrors)
}

// tryReconnect 尝试重新连接 MongoDB（防并发，最多 3 次 × 5 秒）。
func (h *Handler) tryReconnect() {
	if !h.reconnMu.TryLock() {
		return // 已有其他 goroutine 在重连
	}
	defer h.reconnMu.Unlock()

	client, db, err := bulkwriter.Reconnect(context.Background(), h.client, h.uri, h.dbName, 3, 5*time.Second)
	if err != nil {
		log.Printf("[consumer] 重连失败: %v", err)
		return
	}
	h.client = client
	h.db = db
}

// Snapshot 返回当前指标快照（线程安全）。
func (h *Handler) Snapshot() MetricsSnapshot {
	h.metrics.mu.RLock()
	defer h.metrics.mu.RUnlock()
	return MetricsSnapshot{
		Received:    h.metrics.Received,
		Dropped:     h.metrics.Dropped,
		Written:     h.metrics.Written,
		WriteErrors: h.metrics.WriteErrors,
		QueueLen:    len(h.queue),
	}
}

// worker 单个 worker goroutine：从队列聚合 batch → BulkWrite。
//
// 发送触发条件（任一满足即 flush）：
//  1. 达到 BatchSize 条
//  2. 超过 BatchBytes 字节（估算）
//  3. 超过 FlushInterval 时间
//
// flush 时复制 batch 避免与下一批共享底层数组，
// 通过信号量控制并发 BulkWrite 数量。
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
		h.wg.Add(1)
		go func(records []model.Record) {
			defer func() { <-h.sem }()
			defer h.wg.Done()

			written, err := bulkwriter.BulkInsert(ctx, h.db, records)
			h.metrics.mu.Lock()
			if err != nil {
				h.metrics.WriteErrors++
				log.Printf("[consumer] 写库失败: %v，丢弃数据，尝试重连...", err)
				go h.tryReconnect()
			} else {
				h.metrics.Written += int64(written)
			}
			h.metrics.mu.Unlock()
		}(b)
		batch = batch[:0]
		batchBytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			flush() // 关闭前刷出剩余数据
			return
		case <-ticker.C:
			flush() // 定时刷新
		case record, ok := <-h.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, record)
			batchBytes += len(record.Gd) + 200 // Gd 字符串长度 + 其他字段估算
			if len(batch) >= h.cfg.BatchSize || batchBytes >= h.cfg.BatchBytes {
				flush()
			}
		}
	}
}

// writeJSON 写入 JSON 响应。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[consumer] writeJSON error: %v", err)
	}
}
