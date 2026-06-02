// 消费者使用示例：在本项目中接收生产者批量数据并写入 MongoDB。
// 包含 worker 池、并发控制、鉴权、Schema 注册、索引自动创建。

package consumer_test

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/acoderup/mongo-bulkwriter"
	"github.com/acoderup/mongo-bulkwriter/consumer"
)

const authToken = "my-secret-token"

func Example() {
	client, db, _ := bulkwriter.ConnectMongo(context.Background(),
		"mongodb://127.0.0.1:27017", "qstar-history")
	defer client.Disconnect(context.Background())

	// 注册 Schema：一行指定索引字段
	bulkwriter.RegisterSchema("logs", "ops", "psid", "-created_at")

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
	log.Fatal(http.ListenAndServe(":803", nil))
}

func Example_bulkInsert() {
	_, db, _ := bulkwriter.ConnectMongo(context.Background(),
		"mongodb://127.0.0.1:27017", "qstar-history")

	// 注册 Schema
	bulkwriter.RegisterSchema("logs", "ops", "psid", "-created_at")

	// 用 NewRecord 快速写入
	bulkwriter.BulkInsert(context.Background(), db, []bulkwriter.Record{
		bulkwriter.NewRecord("logs", map[string]interface{}{"ops": "test", "psid": "1", "gd": "hello"}),
		bulkwriter.NewRecord("logs", map[string]interface{}{"ops": "test", "psid": "1", "gd": "world"}),
	})
}
