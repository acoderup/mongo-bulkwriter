// 生产者使用示例：其他服务导入 producer 包，通过 HTTP 批量发送数据到消费者。
// 不依赖 MongoDB，内置本地缓冲 + 鉴权 + 空字段校验 + 重试。

package producer_test

import (
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/acoderup/mongo-bulkwriter/producer"
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

	// 有效记录：业务字段通过 Fields 传入
	client.Send(producer.Record{
		Collection: "access_logs",
		CreatedAt:  time.Now().UnixMilli(),
		Fields: map[string]interface{}{
			"ops":         "access",
			"psid":        "service_a",
			"producer_id": 1,
			"gd":          "POST /api/verify ip=192.168.1.1 status=200",
		},
	})

	client.Send(producer.Record{
		Collection: "events",
		CreatedAt:  time.Now().UnixMilli(),
		Fields: map[string]interface{}{
			"ops":         "login",
			"psid":        "user_123",
			"producer_id": 1,
			"gd":          `{"event":"user_login","uid":"user_123"}`,
		},
	})

	client.Send(producer.Record{
		Collection: "metrics",
		CreatedAt:  time.Now().UnixMilli(),
		Fields: map[string]interface{}{
			"ops":         "metric",
			"psid":        "monitor",
			"producer_id": 2,
			"gd":          "cpu=45.2 mem=72.1",
		},
	})

	// 无效记录：Collection 为空 → 丢弃 + 错误日志
	client.Send(producer.Record{
		Fields: map[string]interface{}{
			"ops":  "test",
			"psid": "x",
			"gd":   "missing collection",
		},
	})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("生产者退出")
}
