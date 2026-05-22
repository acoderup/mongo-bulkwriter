// 消费者使用示例：在本项目中接收生产者批量数据并写入 MongoDB。
// 包含 worker 池、并发控制、鉴权、空字段校验、索引自动创建。

package consumer_test

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/acoderup/mongo-bulkwriter"
	"github.com/acoderup/mongo-bulkwriter/consumer"
	"github.com/acoderup/mongo-bulkwriter/model"
)

const authToken = "my-secret-token"

func Example() {
	client, db, _ := bulkwriter.ConnectMongo(context.Background(),
		"mongodb://127.0.0.1:27017", "qstar-history")
	defer client.Disconnect(context.Background())

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

	bulkwriter.BulkInsert(context.Background(), db, []model.Record{
		{Collection: "logs", Ops: "test", PSid: "1", Gd: "hello", CreatedAt: time.Now().UnixMilli()},
		{Collection: "logs", Ops: "test", PSid: "1", Gd: "world", CreatedAt: time.Now().UnixMilli()},
	})
}
