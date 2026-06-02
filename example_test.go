// mongo-bulkwriter 完整使用示例。
// 展示生产者、消费者、直写、查询四端的完整用法。

package bulkwriter_test

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/acoderup/mongo-bulkwriter"
	"github.com/acoderup/mongo-bulkwriter/consumer"
	"github.com/acoderup/mongo-bulkwriter/producer"
)

const authToken = "my-secret-token"

// ==========================================
// 消费者端（本项目）
// ==========================================
func Example_consumer() {
	client, db, _ := bulkwriter.ConnectMongo(
		context.Background(),
		"mongodb://127.0.0.1:27017",
		"qstar-history",
	)
	defer client.Disconnect(context.Background())

	// 注册 Schema：一行指定索引字段
	bulkwriter.RegisterSchema("bets", "ops", "psid", "producer_id", "tid", "-created_at")

	handler := consumer.NewHandler(client, db, "mongodb://127.0.0.1:27017", "qstar-history", consumer.Config{
		AuthToken:     authToken,
		Workers:       32,
		BatchSize:     500,
		BatchBytes:    8 << 20,
		FlushInterval: 100 * time.Millisecond,
		QueueSize:     50000,
		MaxConcurrent: 16,
		MaxBodySize:   10 << 20,
	})
	defer handler.Shutdown()

	http.Handle("/bulkwriter/ingest", handler)
	go http.ListenAndServe(":803", nil)

	log.Println("消费者已启动（鉴权已开启）...")
}

// ==========================================
// 生产者端（其他服务）
// ==========================================
func Example_producer() {
	client := producer.New(producer.Config{
		ConsumerURL:   "http://127.0.0.1:803/bulkwriter/ingest",
		AuthToken:     authToken,
		FlushInterval: 100 * time.Millisecond,
		BatchSize:     500,
		MaxBatchBytes: 8 << 20,
		QueueSize:     100000,
	})
	defer client.Close()

	// 有效记录：完整投注信息（新格式，通过 Fields 传入业务字段）
	client.Send(producer.Record{
		Collection: "bets",
		CreatedAt:  time.Now().UnixMilli(),
		Fields: map[string]interface{}{
			"ops":         "bet",
			"psid":        "session_123",
			"producer_id": 1,
			"tba":         100.0,
			"tid":         "txn_001",
			"twla":        95.0,
			"gd":          `{"gid":126,"cc":"VND","gtba":100,"gtwla":95}`,
		},
	})

	// 无效记录：Collection 为空 → 丢弃 + 日志
	client.Send(producer.Record{
		Fields: map[string]interface{}{
			"ops":  "bet",
			"psid": "session_123",
			"gd":   "missing collection",
		},
	})

	log.Println("生产者已发送数据")
}

// ==========================================
// 本服务内直接写入 Mongo（不经 HTTP）
// ==========================================
func Example_directWrite() {
	_, db, _ := bulkwriter.ConnectMongo(
		context.Background(),
		"mongodb://127.0.0.1:27017",
		"qstar-history",
	)

	// 注册 Schema
	bulkwriter.RegisterSchema("api_logs", "ops", "psid", "tid", "-created_at")

	// 用 NewRecord 快速创建记录
	bulkwriter.BulkInsert(context.Background(), db, []bulkwriter.Record{
		bulkwriter.NewRecord("api_logs", map[string]interface{}{
			"ops":  "verify",
			"psid": "session_1",
			"tid":  "txn_101",
			"gd":   `{"gid":126}`,
		}),
		bulkwriter.NewRecord("api_logs", map[string]interface{}{
			"ops":  "report",
			"psid": "session_1",
			"tid":  "txn_102",
			"gd":   `{"gid":126}`,
		}),
	})
}

// ==========================================
// 查询
// ==========================================
func Example_query() {
	_, db, _ := bulkwriter.ConnectMongo(
		context.Background(),
		"mongodb://127.0.0.1:27017",
		"qstar-history",
	)

	// 按 ops 和 psid 查询，分页
	result, _ := bulkwriter.Query(context.Background(), db, bulkwriter.QueryParams{
		Collection: "bets",
		Ops:        "bet",
		PSid:       "session_123",
		Limit:      20,
		Skip:       0,
	})
	_ = result.Records
	_ = result.Total

	// 按 tid 精确查询
	result2, _ := bulkwriter.Query(context.Background(), db, bulkwriter.QueryParams{
		Collection: "bets",
		Tid:        "txn_001",
	})
	_ = result2

	// 按时间范围查询
	result3, _ := bulkwriter.Query(context.Background(), db, bulkwriter.QueryParams{
		Collection:    "bets",
		CreatedAfter:  time.Now().Add(-24 * time.Hour).UnixMilli(),
		CreatedBefore: time.Now().UnixMilli(),
		Limit:         50,
	})
	_ = result3

	log.Println("查询完成")
}
