# mongo-bulkwriter

高吞吐 MongoDB 异步批量写入 Go 库。**生产者和消费者分离部署，通过 HTTP 建立稳定连接。**

```text
Producer (其他服务) ──HTTP POST──→ Consumer (本项目) ──BulkWrite──→ MongoDB
```

## 安装

```bash
go get github.com/USERNAME/mongo-bulkwriter
```

## 快速开始

### 消费者端

```go
import (
    "github.com/USERNAME/mongo-bulkwriter"
    "github.com/USERNAME/mongo-bulkwriter/consumer"
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

// 4. 本服务内直接写入
bulkwriter.BulkInsert(ctx, db, []model.Record{{
    Collection: "logs", Ops: "test", Pid: "1",
    Data: "...", CreatedAt: time.Now().UnixMilli(),
}})
```

### 生产者端

```go
import "github.com/USERNAME/mongo-bulkwriter/producer"

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
    Collection: "logs",
    Ops:        "access",
    Pid:        "service_a",
    ProducerID: 1,
    Data:       "...",
    CreatedAt:  time.Now().UnixMilli(),
})
```

### 查询

```go
result, _ := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
    Collection: "logs",
    Ops:        "access",
    Pid:        "service_a",
    ProducerID: 1,
    Limit:      100,
})
```

## 消费者内部架构

```
HTTP POST
  → 鉴权 (X-Auth-Token, constant-time compare)
  → 空字段校验 (Collection/Ops/Pid 为空则丢弃)
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
// 消费者
handler := consumer.NewHandler(db, consumer.Config{AuthToken: "my-secret"})

// 生产者
client := producer.New(producer.Config{AuthToken: "my-secret"})
```

生产者自动携带 `X-Auth-Token` 头，消费者用 `crypto/subtle.ConstantTimeCompare` 防时序攻击。

## 生产者发送流程

1. 本地缓冲 100000 条
2. 满足任一条件批量发送：500 条 / 8MB / 100ms
3. 失败自动重试 3 次，退避 100ms → 200ms → 300ms
4. `Collection`/`Ops`/`Pid` 为空时丢弃并打印错误日志
5. 队列满直接丢弃

## 字段校验

生产者 `Send()` 和消费者 `Ingest()` 均校验 `Collection`、`Ops`、`Pid` 非空，为空则丢弃。

## 索引

首次写入集合时自动创建，进程内缓存避免重复查询 Mongo。重启后首次访问重新查询。

| 索引 | 查询场景 |
|------|---------|
| `ops` | 按操作类型 |
| `pid` | 按项目标识 |
| `producer_id` | 按生产者 |
| `{ops, pid}` | 操作 + 项目 |
| `{ops, producer_id}` | 操作 + 生产者 |
| `{pid, producer_id}` | 项目 + 生产者 |
| `{ops, pid, producer_id}` | 三者同时匹配 |
| `{created_at: -1}` | 时间倒序 |

## 数据结构

```go
type Record struct {
    Collection string `json:"collection"`  // 目标集合（必填）
    Ops        string `json:"ops"`         // 操作标识（必填）
    Pid        string `json:"pid"`         // 项目标识（必填）
    ProducerID int    `json:"producer_id"` // 生产者编号
    Data       string `json:"data"`        // 业务数据（字符串）
    CreatedAt  int64  `json:"created_at"`  // Unix 毫秒时间戳
}
```

## API

### 根包

```go
// 连接
client, db, _ := bulkwriter.ConnectMongo(ctx, uri, dbName)

// 写入（按 Collection 分组 unordered BulkWrite，首次写入自动创建索引）
written, _ := bulkwriter.BulkInsert(ctx, db, records)

// 查询
result, _ := bulkwriter.Query(ctx, db, QueryParams{...})

// 索引（可选显式调用）
bulkwriter.EnsureIndexes(ctx, db, "coll_a")
```

### 消费者 `consumer.Handler`

```go
handler := consumer.NewHandler(db, consumer.Config{...})
handler.ServeHTTP(w, r)  // 或 gin.WrapH(handler)
handler.Shutdown()        // 优雅关闭
handler.MetricsSnapshot() // 获取指标
```

### 生产者 `producer.Client`

```go
client := producer.New(producer.Config{...})
client.Send(record) // 非阻塞，返回 bool
client.Close()      // 等待最后一批发送
```

## 配置参数

### 消费者

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `AuthToken` | `""` | 鉴权令牌（空不校验） |
| `Workers` | 32 | worker goroutine 数 |
| `BatchSize` | 500 | 批量条数 |
| `BatchBytes` | 8MB | 批量字节上限 |
| `FlushInterval` | 100ms | flush 超时 |
| `QueueSize` | 50000 | 内部缓冲队列 |
| `MaxConcurrent` | 16 | 最大并发 BulkWrite |
| `MaxBodySize` | 10MB | 请求体硬限制 |

### 生产者

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ConsumerURL` | — | 消费者地址 |
| `AuthToken` | `""` | 鉴权令牌 |
| `FlushInterval` | 100ms | 发送间隔 |
| `BatchSize` | 500 | 批量条数 |
| `MaxBatchBytes` | 8MB | 批量字节上限 |
| `QueueSize` | 100000 | 本地缓冲队列 |

## Docker 部署

```yaml
services:
  qstar-verify:
    image: qstar-verify:latest
    network_mode: host
    environment:
      - MONGO_URI=mongodb://127.0.0.1:27017
    stop_grace_period: 10s

  qstar-mongo:
    image: mongo:7
    network_mode: host
    command: ["mongod", "--nojournal"]
    volumes:
      - /data/mongo:/data/db
```
