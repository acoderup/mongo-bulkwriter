// 生产者使用示例：其他服务导入 producer 包，通过 HTTP 批量发送数据到消费者。
// 不依赖 MongoDB，内置本地缓冲 + 鉴权 + 空字段校验 + 重试。

package producer_test

import (
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/USERNAME/mongo-bulkwriter/producer"
)

const authToken = "my-secret-token"

func Example() {
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
		Collection: "access_logs",
		Ops:        "access",
		Pid:        "service_a",
		ProducerID: 1,
		Data:       "POST /api/verify ip=192.168.1.1 status=200",
		CreatedAt:  time.Now().UnixMilli(),
	})

	// 有效记录（Data 为 JSON 字符串）
	client.Send(producer.Record{
		Collection: "events",
		Ops:        "login",
		Pid:        "user_123",
		ProducerID: 1,
		Data:       `{"event":"user_login","uid":"user_123"}`,
		CreatedAt:  time.Now().UnixMilli(),
	})

	// 有效记录
	client.Send(producer.Record{
		Collection: "metrics",
		Ops:        "metric",
		Pid:        "monitor",
		ProducerID: 2,
		Data:       "cpu=45.2 mem=72.1",
		CreatedAt:  time.Now().UnixMilli(),
	})

	// 无效记录：Collection 为空 → 丢弃 + 错误日志
	client.Send(producer.Record{
		Ops:   "test",
		Pid:   "x",
		Data:  "missing collection",
	})

	// 无效记录：Ops 为空 → 丢弃 + 错误日志
	client.Send(producer.Record{
		Collection: "test",
		Pid:        "x",
		Data:       "missing ops",
	})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("生产者退出")
}
