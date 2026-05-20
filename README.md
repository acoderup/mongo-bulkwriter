# mongo-bulkwriter

高吞吐 MongoDB 异步批量写入与查询 Go 库。**生产者和消费者分离部署，通过 HTTP 建立稳定连接。**

```text
Producer (其他服务) ──HTTP POST──→ Consumer (本项目) ──BulkWrite──→ MongoDB
                                              ↑
                                          Query (直接查询)
```

## 安装

```bash
go get github.com/acoderup/mongo-bulkwriter
```

## 快速开始

### 消费者端（本项目）

```go
import (
    "github.com/acoderup/mongo-bulkwriter"
    "github.com/acoderup/mongo-bulkwriter/consumer"
)

// 1. 连接 MongoDB
client, db, _ := bulkwriter.ConnectMongo(ctx, "mongodb://127.0.0.1:27017", "qstar-history")
defer client.Disconnect(ctx)

// 2. 创建消费者（内置 worker 池 + 并发控制 + 鉴权）
handler := consumer.NewHandler(db, consumer.Config{
    AuthToken:     "your-secret-token",
    Workers:       32,
    BatchSize:     500,
    BatchBytes:    8 << 20,
    FlushInterval: 100 * time.Millisecond,
    QueueSize:     50000,
    MaxConcurrent: 16,
    MaxBodySize:   10 << 20,
})
defer handler.Shutdown()

// 3. 注册接口
r.POST("/bulkwriter/ingest", gin.WrapH(handler))
```

### 生产者端（其他服务）

```go
import "github.com/acoderup/mongo-bulkwriter/producer"

client := producer.New(producer.Config{
    ConsumerURL:   "http://127.0.0.1:803/bulkwriter/ingest",
    AuthToken:     "your-secret-token",
    FlushInterval: 100 * time.Millisecond,
    BatchSize:     500,
    MaxBatchBytes: 8 << 20,
    QueueSize:     100000,
})
defer client.Close()

client.Send(producer.Record{
    Collection: "bets",
    Ops:        "bet",
    PSid:       "session_123",
    ProducerID: 1,
    Tba:        100.0,
    Tid:        "txn_001",
    Twla:       95.0,
    Gd:         `{"gid":126,"cc":"VND"}`,
    CreatedAt:  time.Now().UnixMilli(),
})
```

### 直接写入

```go
bulkwriter.BulkInsert(ctx, db, []model.Record{{
    Collection: "logs",
    Ops:        "test",
    PSid:       "s1",
    Tid:        "txn_1",
    Gd:         `{"msg":"hello"}`,
    CreatedAt:  time.Now().UnixMilli(),
}})
```

### 查询

```go
// 按条件分页查询
result, _ := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
    Collection: "bets",
    Ops:        "bet",
    PSid:       "session_123",
    Limit:      20,
    Skip:       0,
})

// 按 tid 精确查询
result, _ := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
    Collection: "bets",
    Tid:        "txn_001",
})

// 按时间范围查询
result, _ := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
    Collection:    "bets",
    CreatedAfter:  time.Now().Add(-24 * time.Hour).UnixMilli(),
    CreatedBefore: time.Now().UnixMilli(),
    Limit:         50,
})

// 结果
fmt.Println(result.Total)    // 符合条件的总记录数
for _, rec := range result.Records {
    fmt.Println(rec.Tid, rec.Tba, rec.Twla, rec.Gd)
}
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

过载: 队列满 → 429 Too Many Requests
关闭: handler.Shutdown() → 等待队列排空 → worker 退出
```

## 鉴权

```go
// 消费者 — 配置 AuthToken 开启鉴权
handler := consumer.NewHandler(db, consumer.Config{AuthToken: "my-secret"})

// 生产者 — 配置相同 AuthToken，自动携带 X-Auth-Token 头
client := producer.New(producer.Config{AuthToken: "my-secret"})
```

消费者使用 `crypto/subtle.ConstantTimeCompare` 进行恒定时间比较，防止时序攻击。

## 生产者发送流程

1. `Send()` 非阻塞写入本地缓冲队列（默认 100000 条）
2. 满足任一条件触发批量 HTTP POST：
   - 达到 BatchSize 条（默认 500）
   - 超过 MaxBatchBytes 字节（默认 8MB）
   - 超过 FlushInterval 时间（默认 100ms）
3. 失败自动重试 3 次，递增退避 100ms → 200ms → 300ms
4. `Collection`/`Ops`/`PSid` 为空时丢弃并打印错误日志
5. 队列满直接丢弃

## 字段校验

| 位置 | 校验字段 | 行为 |
|------|---------|------|
| `producer.Send()` | Collection, Ops, PSid | 空则丢弃 + 错误日志 |
| `consumer.Ingest()` | Collection, Ops, PSid | 空则丢弃 + 指标记录 |

## 索引

首次写入集合时自动创建，进程内缓存避免重复查询 Mongo。重启后首次访问重新检查。

| 索引 | 用途 |
|------|------|
| `{ops: 1}` | 按操作类型查询 |
| `{psid: 1}` | 按项目/会话标识查询 |
| `{producer_id: 1}` | 按生产者编号查询 |
| `{tid: 1}` | 按记录 ID 精确查询 |
| `{ops: 1, psid: 1}` | 操作 + 项目联合查询 |
| `{ops: 1, producer_id: 1}` | 操作 + 生产者联合查询 |
| `{psid: 1, producer_id: 1}` | 项目 + 生产者联合查询 |
| `{ops: 1, psid: 1, producer_id: 1}` | 三者联合查询 |
| `{created_at: -1}` | 时间倒序排序 |

## 数据结构

### Record（传输用，JSON 序列化）

```go
type Record struct {
    Collection string  `json:"collection"`  // 目标集合名（必填）
    Ops        string  `json:"ops"`         // 操作标识（必填）
    PSid       string  `json:"psid"`        // 项目/会话标识（必填）
    ProducerID int     `json:"producer_id"` // 生产者编号
    Tba        float64 `json:"tba"`         // 总投注金额
    Tid        string  `json:"tid"`         // 记录唯一ID
    Twla       float64 `json:"twla"`        // 总赢输金额
    Gd         string  `json:"gd"`          // 业务数据（JSON 字符串）
    CreatedAt  int64   `json:"created_at"`  // 创建时间（Unix 毫秒）
}
```

### DocRecord（存储/查询用，BSON 序列化）

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

### QueryParams（查询参数）

```go
type QueryParams struct {
    Collection    string // 集合名（为空默认 "default"）
    Ops           string // 按操作类型筛选
    PSid          string // 按项目标识筛选
    ProducerID    int    // 按生产者编号筛选（0 不筛选）
    Tid           string // 按记录ID筛选
    CreatedAfter  int64  // created_at >= 此值（Unix 毫秒）
    CreatedBefore int64  // created_at <= 此值（Unix 毫秒）
    Limit         int64  // 返回条数（默认 100）
    Skip          int64  // 跳过条数（分页用）
}
```

## API

### 根包 `bulkwriter`

```go
// 连接 MongoDB（高吞吐配置：300 最大连接池、Unacknowledged 写入）
client, db, _ := bulkwriter.ConnectMongo(ctx, uri, dbName)

// 批量写入（按 Collection 分组、unordered、自动创建索引）
written, _ := bulkwriter.BulkInsert(ctx, db, records)

// 条件查询（支持多条件筛选、时间范围、分页）
result, _ := bulkwriter.Query(ctx, db, QueryParams{...})

// 显式创建索引（通常不需要手动调用，BulkInsert 自动处理）
bulkwriter.EnsureIndexes(ctx, db, "coll_a")
```

### 消费者 `consumer.Handler`

```go
handler := consumer.NewHandler(db, consumer.Config{...})
handler.ServeHTTP(w, r)  // 或 gin.WrapH(handler)
handler.Shutdown()        // 优雅关闭（等待队列清空）
handler.MetricsSnapshot() // 获取运行指标
```

### 生产者 `producer.Client`

```go
client := producer.New(producer.Config{...})
client.Send(record) // 非阻塞发送，返回 bool
client.Close()      // 等待最后一批发送完成
```

### 查询结果 `QueryResult`

```go
result.Records // []DocRecord — 类型化的查询结果
result.Total   // int64 — 符合条件的总记录数（不受 Limit/Skip 影响）
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

## 吞吐量分析

### 目标：1 万条/秒

| 层级 | 配置 | 理论吞吐 | 说明 |
|------|------|---------|------|
| MongoDB BulkWrite | Unacknowledged, 300 连接池 | **50 万+ rec/s** | unordered 写入，不等待确认 |
| Consumer worker 池 | 32 workers, 16 并发, 500/批 | **~8 万 rec/s** | 每 100ms 可刷 16×500=8000 条 |
| Consumer 队列缓冲 | 50000 容量 | 可吸收 **5 秒**突发 | 队列满前不丢数据 |
| Producer HTTP | 500/批, 100ms flush | **~5000 rec/s** | 单客户端，可水平扩展 |

**结论：1 万条/秒完全达标。** Consumer 端理论吞吐远高于需求。

**瓶颈分析：**
- 最大瓶颈在 **Producer HTTP 发送端**（单客户端 ~5000/s）
- 突破方式：增加 Producer 实例数，或调大 `BatchSize`/减小 `FlushInterval`
- MongoDB 写入在 Unacknowledged 模式下几乎不是瓶颈
- Consumer 队列 (50000) 提供 5 秒缓冲，可应对短时突发

**实测：**
```bash
cd mongo-bulkwriter
go test -v -run TestThroughput_10k -timeout 60s   # 1 万条
go test -v -run TestThroughput_100k -timeout 120s  # 10 万条
```
