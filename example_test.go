// mongo-bulkwriter 完整使用示例。
// 展示生产者（其他服务）和消费者（本项目）如何配合使用，包含鉴权。

package bulkwriter_test

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/USERNAME/mongo-bulkwriter"
	"github.com/USERNAME/mongo-bulkwriter/consumer"
	"github.com/USERNAME/mongo-bulkwriter/model"
	"github.com/USERNAME/mongo-bulkwriter/producer"
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

	handler := consumer.NewHandler(db, consumer.Config{
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

	// 有效记录
	client.Send(producer.Record{
		Collection: "logs",
		Ops:        "access",
		Pid:        "service_a",
		ProducerID: 1,
		Data:       "POST /api/verify status=200",
		CreatedAt:  time.Now().UnixMilli(),
	})

	// 无效记录：Collection 为空 → 丢弃 + 日志
	client.Send(producer.Record{
		Ops:   "access",
		Pid:   "service_a",
		Data:  "missing collection",
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

	bulkwriter.BulkInsert(context.Background(), db, []model.Record{
		{Collection: "api_logs", Ops: "verify", Pid: "126", Data: "...", CreatedAt: time.Now().UnixMilli()},
		{Collection: "api_logs", Ops: "report", Pid: "126", Data: "...", CreatedAt: time.Now().UnixMilli()},
	})
}
