# mongo-bulkwriter

高吞吐 MongoDB 批量写入与查询 Go 库。生产者和消费者分离部署，通过 HTTP 解耦。

```text
Producer (其他服务) ──HTTP POST──→ Consumer (本项目) ──BulkWrite──→ MongoDB
                                             ↑
                              Query / FindOne / DeleteOldRecords (直接查询/清理)
```

## 安装

```bash
go get github.com/acoderup/mongo-bulkwriter
```

## 包结构

| 包 | 用途 |
|------|------|
| `bulkwriter`（根包） | 直连 MongoDB：连接、批量写入、查询、删除、索引管理 |
| `consumer` | 消费者 HTTP 服务端，接收批量数据异步写入 |
| `producer` | 生产者 HTTP 客户端，本地缓冲后批量发送 |
| `cleanup` | 定时清理调度器，按天数自动删除过期数据 |

## 快速开始

### 消费者端

```go
import (
    "github.com/acoderup/mongo-bulkwriter"
    "github.com/acoderup/mongo-bulkwriter/consumer"
)

// 1. 连接 MongoDB
client, db, _ := bulkwriter.ConnectMongo(ctx, "mongodb://127.0.0.1:27017", "qstar-history")
defer client.Disconnect(ctx)

// 2. 创建消费者（内置 worker 池、并发控制、重连、鉴权）
handler := consumer.NewHandler(client, db, "mongodb://127.0.0.1:27017", "qstar-history",
    consumer.Config{
        AuthToken:     "your-secret-token",
        Workers:       32,
        BatchSize:     500,
        FlushInterval: 100 * time.Millisecond,
        QueueSize:     50000,
        MaxConcurrent: 16,
    })
defer handler.Shutdown()

// 3. 注册路由
r.POST("/bulkwriter/ingest", gin.WrapH(handler))
```

> `NewHandler` 需要 `client`、`uri`、`dbName` 参数用于写库失败时的自动重连。

### 生产者端

```go
import "github.com/acoderup/mongo-bulkwriter/producer"

client := producer.New(producer.Config{
    ConsumerURL: "http://127.0.0.1:803/bulkwriter/ingest",
    AuthToken:   "your-secret-token",
})
defer client.Close()

client.Send(producer.Record{
    Collection: "bets",
    Ops:        "bet",
    PSid:       "session_123",
    Tba:        100.0,
    Tid:        "txn_001",
    Twla:       95.0,
    Gd:         `{"gid":126,"cc":"VND"}`,
    CreatedAt:  time.Now().UnixMilli(),
})
```

### 直接写入（不走 HTTP）

```go
bulkwriter.BulkInsert(ctx, db, []model.Record{{
    Collection: "logs",
    Ops:        "test",
    PSid:       "s1",
    Gd:         `{"msg":"hello"}`,
    CreatedAt:  time.Now().UnixMilli(),
}})
```

### 查询

```go
// 按条件分页查询（按 psid 分组）
result, _ := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
    Collection: "bets",
    Ops:        "bet",
    Limit:      20,
    Skip:       0,
})

// 按 tid 精确查询
doc, _ := bulkwriter.FindOne(ctx, db, "bets", bson.M{"tid": "txn_001"})

// 按时间范围查询
result, _ := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
    Collection:    "bets",
    CreatedAfter:  time.Now().Add(-24 * time.Hour).UnixMilli(),
    CreatedBefore: time.Now().UnixMilli(),
})
```

### 删除过期数据

```go
// 直接调用：删除 30 天前的数据
cutoff := time.Now().AddDate(0, 0, -30).UnixMilli()
deleted, _ := bulkwriter.DeleteOldRecords(ctx, db, "bets", cutoff)
```

### 定时清理

```go
import "github.com/acoderup/mongo-bulkwriter/cleanup"

// 每 72 小时清理一次 100 天前的数据
stop := cleanup.Start(ctx, db, cleanup.Config{
    RetentionDays: 100,
    IntervalHours: 72,
})
defer stop()
```

## 消费者内部架构

```
HTTP POST
  → 鉴权 (X-Auth-Token, constant-time compare)
  → 空字段校验 (Collection/Ops/PSid 为空则丢弃)
  → 非阻塞入队 (202 Accepted)
  → 内部队列 (50000)
  → 32 workers
    → batch: 500条 / 8MB / 100ms 任一触发 flush
    → semaphore (最多 16 并发 BulkWrite)
    → Mongo unordered BulkWrite
  → 写库失败 → 丢弃数据 → 异步重连 (3次 × 5秒)

过载: 队列满 → 429 Too Many Requests
关闭: handler.Shutdown() → 等待队列清空 → worker 退出
```

## 重连机制

消费者写库失败时自动尝试重连：断开旧连接 → 重试 `ConnectMongo`，最多 3 次、每次 5 秒超时。重连期间继续接收新数据（旧批次已丢弃）。

## 鉴权

消费者使用 `crypto/subtle.ConstantTimeCompare` 恒定时间比较，防止时序攻击。

```go
// 消费者
handler := consumer.NewHandler(client, db, uri, dbName, consumer.Config{
    AuthToken: "my-secret",
})

// 生产者 — 自动携带 X-Auth-Token
client := producer.New(producer.Config{AuthToken: "my-secret"})
```

## 索引

首次写入集合时自动创建，`sync.Map` 进程内缓存避免重复检查。

| 索引 | 用途 |
|------|------|
| `{ops: 1}` | 按操作类型查询 |
| `{psid: 1}` | 按项目/会话标识查询 |
| `{producer_id: 1}` | 按生产者编号查询 |
| `{tid: 1}` | 按记录 ID 精确查询 |
| `{created_at: -1}` | 时间倒序排序 + 过期删除 |
| `{ops, psid}` / `{ops, producer_id}` / `{psid, producer_id}` | 联合查询 |
| `{ops, psid, producer_id}` | 三者联合查询 |

## 数据结构

### Record（传输用，JSON）

```go
type Record struct {
    Collection string  `json:"collection"`  // 目标集合名（必填）
    Ops        string  `json:"ops"`         // 操作标识（必填）
    PSid       string  `json:"psid"`        // 项目/会话标识（必填）
    ProducerID int     `json:"producer_id"`
    Tba        float64 `json:"tba"`
    Tid        string  `json:"tid"`
    Twla       float64 `json:"twla"`
    Gd         string  `json:"gd"`          // 业务数据（JSON 字符串）
    CreatedAt  int64   `json:"created_at"`  // Unix 毫秒
}

// Record → DocRecord 转换
func (r Record) ToDocRecord() DocRecord
```

### DocRecord（存储/查询用，BSON）

```go
type DocRecord struct {
    Ops        string  `bson:"ops"`
    PSid       string  `bson:"psid"`
    ProducerID int     `bson:"producer_id"`
    Tba        float64 `bson:"tba"`
    Tid        string  `bson:"tid"`
    Twla       float64 `bson:"twla"`
    Gd         string  `bson:"gd"`
    CreatedAt  int64   `bson:"created_at"`
}
```

### QueryParams / QueryResult

```go
type QueryParams struct {
    Collection    string // 集合名
    Ops           string
    PSid          string
    ProducerID    int
    Tid           string
    CreatedAfter  int64  // created_at >= (Unix 毫秒)
    CreatedBefore int64  // created_at <= (Unix 毫秒)
    Limit         int64  // 默认 100
    Skip          int64
}

type QueryResult struct {
    Records []DocRecord // 当前页全部记录
    Total   int64       // 去重后的 psid 总数
}
```

## 全部 API

### 根包 `bulkwriter`

```go
client, db, _ := bulkwriter.ConnectMongo(ctx, uri, dbName)
client, db, _ := bulkwriter.Reconnect(ctx, oldClient, uri, dbName, 3, 5*time.Second)
written, _    := bulkwriter.BulkInsert(ctx, db, records)
result, _     := bulkwriter.Query(ctx, db, params)
doc, _        := bulkwriter.FindOne(ctx, db, collection, filter)
deleted, _    := bulkwriter.DeleteOldRecords(ctx, db, collection, before)
collections, _:= bulkwriter.ListCollections(ctx, db)
_              = bulkwriter.EnsureIndexes(ctx, db, "coll_a")
```

### consumer

```go
handler := consumer.NewHandler(client, db, uri, dbName, cfg)
handler.ServeHTTP(w, r)    // 或 gin.WrapH(handler)
snap := handler.Snapshot() // MetricsSnapshot（不含 mutex）
handler.Shutdown()         // 等待队列清空
```

### producer

```go
client := producer.New(cfg)
ok := client.Send(record) // 非阻塞，返回成功/失败
client.Close()            // 等待最后一批发送完成
```

### cleanup

```go
stop := cleanup.Start(ctx, db, cleanup.Config{
    RetentionDays: 100,  // 保留天数
    IntervalHours: 72,   // 执行间隔
})
stop() // 优雅停止
```

## 配置参数

### 消费者 Config

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `AuthToken` | `""` | 鉴权令牌（空不校验） |
| `Workers` | 32 | worker goroutine 数 |
| `BatchSize` | 500 | 批量写入条数 |
| `BatchBytes` | 8MB | 批量字节上限（估算） |
| `FlushInterval` | 100ms | 定时 flush 间隔 |
| `QueueSize` | 50000 | 内部缓冲队列大小 |
| `MaxConcurrent` | 16 | 最大并发 BulkWrite 数 |
| `MaxBodySize` | 10MB | 请求体硬限制 |

### 生产者 Config

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ConsumerURL` | — | 消费者地址（必填） |
| `AuthToken` | `""` | 鉴权令牌 |
| `FlushInterval` | 100ms | 批量发送间隔 |
| `BatchSize` | 500 | 批量条数 |
| `MaxBatchBytes` | 8MB | 批量字节上限（估算） |
| `QueueSize` | 100000 | 本地缓冲队列大小 |

### cleanup Config

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `RetentionDays` | 100 | 保留天数 |
| `IntervalHours` | 72 | 执行间隔（小时） |

## 连接配置

`ConnectMongo` 默认：
- 连接池：最大 300 / 最小 50
- WriteConcern：Unacknowledged（高吞吐，不等待确认）
- 禁用重试写入（避免乱序）

## 吞吐量分析

### 各层级理论吞吐

| 层级 | 计算方式 | 理论吞吐 |
|------|---------|---------|
| MongoDB BulkWrite | Unacknowledged 单次 500 条约 5ms | **10 万+ rec/s** |
| Consumer worker 池 | 16 并发 × 500条/批 ÷ 100ms flush | **~8 万 rec/s** |
| Consumer 队列缓冲 | 50000 容量 ÷ 80000 rec/s | 吸收 **0.6 秒**突发 |
| Producer 单客户端 | 500条/批 ÷ 100ms flush | **~5000 rec/s** |
| Producer 水平扩展 | N 个客户端 × 5000 | **N × 5000 rec/s** |

### Consumer 内部逐层计算

```
HTTP 接收 → 非阻塞入队 (速率 = producer 发送速率)
    ↓
内部队列 50000 (缓冲 = 容量 ÷ 入队速率，默认可吸收 10s @ 5000/s)
    ↓
32 workers 各自聚合 batch
    ↓
flush 条件: 500条 / 8MB / 100ms (任一触发)
    ↓
semaphore (16 并发) → BulkWrite
    ↓
MongoDB Unacknowledged 写入
```

**关键指标：**

| 场景 | 入队速率 | 队列积压 | 写入延迟 | 状态 |
|------|---------|---------|---------|------|
| 轻载 | < 1000/s | < 200 | < 10ms | 正常 |
| 额定 | 5000/s | ~500 | ~20ms | 正常 |
| 高载 | 10000/s | ~2000 | ~50ms | 正常 |
| 过载 | > 80000/s | 队列满 | — | 429 拒绝 |

### Producer 端吞吐分析

单客户端 `500条/批 × 100ms flush = 5000 rec/s`。

**提升方式：**

| 方案 | 效果 | 代价 |
|------|------|------|
| 增大 `BatchSize` → 1000 | 10000 rec/s | HTTP 请求体变大（200KB） |
| 减小 `FlushInterval` → 50ms | 10000 rec/s | HTTP 请求更频繁 |
| 增加 Producer 实例数 | 线性扩展 | 部署复杂度 |
| 组合：BatchSize=1000 + 50ms | 20000 rec/s | 请求体 200KB，每秒 20 次 POST |

### MongoDB 写入性能

`Unacknowledged` 模式下不等待确认，延迟接近网络 RTT。

| 配置 | 单 BulkWrite (500条) 延迟 | 说明 |
|------|--------------------------|------|
| 本地 MongoDB | ~1-3ms | 无网络开销 |
| 同机房 MongoDB | ~3-10ms | 内网 RTT |
| 跨机房 MongoDB | ~10-50ms | 取决于网络延迟 |

**16 并发 × (1000ms ÷ 5ms) = 3200 批/秒 × 500条 = 160 万 rec/s**

MongoDB 写入在 Unacknowledged 模式下基本不是瓶颈，实际瓶颈在 Producer 发送端。

### 端到端推荐配置

| 目标吞吐 | Producer 配置 | Consumer 配置 | Producer 实例数 |
|---------|-------------|-------------|----------------|
| 5000/s | BatchSize=500, Flush=100ms | 默认 | 1 |
| 10000/s | BatchSize=500, Flush=100ms | 默认 | 2 |
| 10000/s | BatchSize=1000, Flush=100ms | 默认 | 1 |
| 20000/s | BatchSize=1000, Flush=50ms | Workers=64, MaxConcurrent=32 | 1 |
| 50000+ | BatchSize=1000, Flush=50ms | Workers=64, MaxConcurrent=32 | 3+ |

## DeleteOldRecords 分批策略

千万级数据不锁库：每批 `Find(_id, limit=10000)` → `DeleteMany(_id in ids)`，批次间检查 `ctx.Done()`。每 10 批输出进度日志。`created_at` 必须有索引。
