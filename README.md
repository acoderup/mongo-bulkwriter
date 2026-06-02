# mongo-bulkwriter

高吞吐 MongoDB 批量写入与查询 Go 库。支持多 Schema 业务结构，生产者和消费者分离部署，通过 HTTP 解耦。

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
| `bulkwriter`（根包） | 直连 MongoDB：连接、批量写入、查询、删除、Schema 注册 |
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

// 2. 注册 Schema（一行指定索引字段）
bulkwriter.RegisterSchema("bets", "ops", "psid", "producer_id", "tid", "-created_at")

// 3. 创建消费者（内置 worker 池、并发控制、重连、鉴权）
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

// 4. 注册路由
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
    CreatedAt:  time.Now().UnixMilli(),
    Fields: map[string]interface{}{
        "ops":  "bet",
        "psid": "session_123",
        "tba":  100.0,
        "tid":  "txn_001",
        "twla": 95.0,
        "gd":   `{"gid":126,"cc":"VND"}`,
    },
})
```

### 直接写入（不走 HTTP）

```go
// 一行注册 Schema
bulkwriter.RegisterSchema("logs", "ops", "psid", "-created_at")

// 用 NewRecord 快速创建记录
bulkwriter.BulkInsert(ctx, db, []bulkwriter.Record{
    bulkwriter.NewRecord("logs", map[string]interface{}{
        "ops":  "test",
        "psid": "s1",
        "gd":   `{"msg":"hello"}`,
    }),
})
```

### 批量配置多个 Schema

```go
bulkwriter.Configure(
    bulkwriter.SchemaConfig{Collection: "bet_logs", Indexes: []string{"ops", "psid", "-created_at"}},
    bulkwriter.SchemaConfig{Collection: "pay_logs", Indexes: []string{"order_id", "user_id", "-created_at"}},
)
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

// 自定义 Filter 筛选（与便捷字段 AND 合并）
result, _ := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
    Collection: "bets",
    Filter:     bson.M{"status": "ok"},
    Limit:      50,
})

// 按时间范围查询
result, _ := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
    Collection:    "bets",
    CreatedAfter:  time.Now().Add(-24 * time.Hour).UnixMilli(),
    CreatedBefore: time.Now().UnixMilli(),
})
```

### 删除过期数据

```go
cutoff := time.Now().AddDate(0, 0, -30).UnixMilli()
deleted, _ := bulkwriter.DeleteOldRecords(ctx, db, "bets", cutoff)
```

### 定时清理

```go
import "github.com/acoderup/mongo-bulkwriter/cleanup"

stop := cleanup.Start(ctx, db, cleanup.Config{
    RetentionDays: 100,
    IntervalHours: 72,
})
defer stop()
```

---

## Schema 与索引

### 概述

索引由 `SchemaRegistry` 驱动。未注册 Schema 的集合自动使用 `DefaultSchema`（向后兼容），自定义 Schema 通过 `RegisterSchema` 或 `Configure` 注册。

### 索引字段规则

索引字段使用纯字符串，内部自动转为 MongoDB 索引键：

| 格式 | 含义 | MongoDB 索引 |
|------|------|-------------|
| `"field"` | 单字段升序 | `{field: 1}` |
| `"-field"` | 单字段降序 | `{field: -1}` |
| `"a,b"` | 复合升序 | `{a: 1, b: 1}` |
| `"-a,-b"` | 复合降序 | `{a: -1, b: -1}` |

**规则：**
- 前缀 `-` 表示降序，不加前缀为升序
- 逗号 `,` 分隔复合索引的多个字段
- 复合索引中的所有字段必须同向（全升或全降），跨向复合索引用 `model.Idx(bson.D{...})` 创建
- 索引创建是幂等的，重复调用不会重复创建
- 首次写入集合时自动创建，`sync.Map` 进程内缓存避免重复检查

### 注册方式

**方式 1：`RegisterSchema` — 一行注册**

```go
bulkwriter.RegisterSchema("pay_logs", "order_id", "user_id", "-created_at")
```

参数：`(collection名称, 索引字段1, 索引字段2, ...)`

**方式 2：`Configure` — 批量注册**

```go
bulkwriter.Configure(
    bulkwriter.SchemaConfig{
        Collection: "bet_logs",
        Indexes:    []string{"ops", "psid", "-created_at"},
    },
    bulkwriter.SchemaConfig{
        Collection: "pay_logs",
        Indexes:    []string{"order_id", "-created_at"},
        Validate: func(r *model.Record) error {
            if r.Fields["order_id"] == nil {
                return errors.New("缺少 order_id")
            }
            return nil
        },
    },
)
```

**方式 3：`SchemaBuilder` — 完整控制**

```go
model.NewSchema("payment").
    WithIndex(model.Asc("order_id")).           // 升序
    WithIndex(model.Desc("created_at"), true).  // 降序 + 唯一索引
    WithValidate(func(r *model.Record) error {
        if r.Fields["order_id"] == nil {
            return errors.New("缺少 order_id")
        }
        return nil
    }).
    Register("pay_logs")
```

只导入根包 `bulkwriter` 时用方式 1 和 2，需要唯一索引或混合向复合索引时用方式 3。

### 默认 Schema

不注册任何 Schema 的集合使用 `DefaultSchema()`，自动创建以下索引：

| 索引 | 用途 |
|------|------|
| `{ops: 1}` | 按操作类型查询 |
| `{psid: 1}` | 按项目/会话标识查询 |
| `{producer_id: 1}` | 按生产者编号查询 |
| `{tid: 1}` | 按记录 ID 精确查询 |
| `{created_at: -1}` | 时间倒序排序 + 过期删除 |
| `{ops, psid}` | 操作 + 会话联合查询 |
| `{ops, producer_id}` | 操作 + 生产者联合查询 |
| `{psid, producer_id}` | 会话 + 生产者联合查询 |
| `{ops, psid, producer_id}` | 三者联合查询 |

### 校验规则

每条记录写入前经过两级校验：

1. **基础设施校验**（始终生效）：`Collection` 为空 → 丢弃
2. **Schema 校验**（可选）：`Schema.Validate` 不为 nil 时执行，返回 error → 丢弃

```go
bulkwriter.SchemaConfig{
    Collection: "pay_logs",
    Indexes:    []string{"order_id", "-created_at"},
    Validate: func(r *model.Record) error {
        if r.Fields["order_id"] == nil || r.Fields["order_id"].(string) == "" {
            return errors.New("order_id 缺失")
        }
        return nil
    },
}
```

---

## 数据结构

### Record（传输用，JSON）

```go
type Record struct {
    Collection string                 `json:"collection"`   // 目标集合名（必填）
    ProducerID int                    `json:"producer_id"`  // 生产者编号
    CreatedAt  int64                  `json:"created_at"`   // Unix 毫秒时间戳
    Fields     map[string]interface{} `json:"fields"`       // 业务字段
}
```

**字段分类：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `Collection` | 基础设施 | 路由到目标 MongoDB 集合，为空时丢弃 |
| `ProducerID` | 基础设施 | 生产者编号，默认 0 |
| `CreatedAt` | 基础设施 | 时间戳，写入 MongoDB 后用于过期清理 |
| `Fields` | 业务字段 | 所有业务数据，由 Schema 定义结构，写入时 inline 到文档根级 |

**JSON 格式兼容：**

新格式（推荐）：
```json
{"collection":"bets","producer_id":1,"created_at":1717200000000,"fields":{"ops":"bet","psid":"s1","tba":100}}
```

旧格式（兼容，自动转换）：
```json
{"collection":"bets","producer_id":1,"ops":"bet","psid":"s1","tba":100,"created_at":1717200000000}
```

旧格式中除 `collection`、`producer_id`、`created_at`、`fields` 之外的所有 key 自动提取到 `Fields`。两种格式写入 MongoDB 后的文档结构完全相同。

### DocRecord（存储/查询用，BSON）

```go
type DocRecord struct {
    ProducerID int                    `bson:"producer_id"`
    CreatedAt  int64                  `bson:"created_at"`
    Fields     map[string]interface{} `bson:",inline"`
}
```

`Fields` 使用 `bson:",inline"` 内联到文档根级，每个业务字段都是独立的 MongoDB 文档字段，可分别建索引。

查询时通过类型断言访问：
```go
ops := doc.Fields["ops"].(string)
tba := doc.Fields["tba"].(float64)
```

### 便捷构造函数

```go
// 自动填入当前时间
rec := bulkwriter.NewRecord("logs", map[string]interface{}{
    "ops":  "test",
    "psid": "s1",
})

// 指定时间 + ProducerID
rec := bulkwriter.NewRecordWithTime("logs", createdAt, map[string]interface{}{
    "ops":  "test",
})
```

### QueryParams / QueryResult

```go
type QueryParams struct {
    Collection    string // 集合名（必填，空默认 "default"）
    Ops           string // 按 ops 筛选（可选）
    PSid          string // 按 psid 筛选（可选）
    ProducerID    int    // 按 producer_id 筛选（可选，0 不筛选）
    Tid           string // 按 tid 筛选（可选）
    CreatedAfter  int64  // created_at >=（可选，Unix 毫秒）
    CreatedBefore int64  // created_at <=（可选，Unix 毫秒）
    Limit         int64  // 返回条数（分组数），默认 100
    Skip          int64  // 跳过条数（分组数）
    Filter        bson.M // 自定义筛选条件，与便捷字段 AND 合并
}

type QueryResult struct {
    Records []DocRecord // 当前页全部记录
    Total   int64       // 去重后 psid 总数
}
```

便捷字段 `Ops`、`PSid`、`ProducerID`、`Tid` 为常用查询提供快捷方式，与 `Filter` 同时设置时按 AND 逻辑合并。

---

## 消费者内部架构

```
HTTP POST
  → 鉴权 (X-Auth-Token, constant-time compare)
  → Schema 校验 (Collection 为空则丢弃，Schema.Validate 可选)
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

---

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

## 全部 API

### 根包 `bulkwriter`

```go
// ── 连接 ──
client, db, _ := bulkwriter.ConnectMongo(ctx, uri, dbName)
client, db, _ := bulkwriter.Reconnect(ctx, oldClient, uri, dbName, maxAttempts, timeout)

// ── Schema 注册 ──
bulkwriter.RegisterSchema("coll", "f1", "f2", "-f3")          // 字符串索引
bulkwriter.Configure(schemaConfigs...)                          // 批量注册
bulkwriter.EnsureIndexes(ctx, db, "coll_a", "coll_b")          // 手动建索引

// ── 写入 ──
rec := bulkwriter.NewRecord("coll", map[string]interface{}{"k": "v"})
rec := bulkwriter.NewRecordWithTime("coll", createdAt, fields)
written, _ := bulkwriter.BulkInsert(ctx, db, records)

// ── 查询 ──
result, _ := bulkwriter.Query(ctx, db, params)
doc, _    := bulkwriter.FindOne(ctx, db, collection, filter)
deleted, _:= bulkwriter.DeleteOldRecords(ctx, db, collection, before)
cols, _   := bulkwriter.ListCollections(ctx, db)
```

### consumer

```go
handler := consumer.NewHandler(client, db, uri, dbName, cfg)
handler.ServeHTTP(w, r)    // 或 gin.WrapH(handler)
snap := handler.Snapshot() // MetricsSnapshot
handler.Shutdown()         // 等待队列清空
```

### producer

```go
client := producer.New(cfg)
ok := client.Send(record)  // 非阻塞
client.Close()             // 等待最后一批发送完成
```

### cleanup

```go
stop := cleanup.Start(ctx, db, cleanup.Config{RetentionDays: 100, IntervalHours: 72})
stop()
```

---

## 配置参数

### 消费者 Config

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `AuthToken` | `""` | 鉴权令牌（空不校验） |
| `Workers` | 32 | worker goroutine 数 |
| `BatchSize` | 500 | 批量写入条数 |
| `BatchBytes` | 8MB | 批量字节上限（基于 Fields 估算） |
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
| `MaxBatchBytes` | 8MB | 批量字节上限（基于 Fields 估算） |
| `QueueSize` | 100000 | 本地缓冲队列大小 |

### cleanup Config

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `RetentionDays` | 100 | 保留天数 |
| `IntervalHours` | 72 | 执行间隔（小时） |

---

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
