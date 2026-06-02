// mongo-bulkwriter 吞吐量基准测试。
// 验证一秒内处理 1 万条数据的能力。
//
// 运行方式（需要本地 MongoDB）：
//   go test -v -run TestThroughput_10k -timeout 60s
//   go test -bench BenchmarkBulkInsert_10k -benchtime=5s

package bulkwriter_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/acoderup/mongo-bulkwriter"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	benchMongoURI  = "mongodb://127.0.0.1:27017"
	benchDBName    = "qstar-bench"
	benchConnectTO = 2 * time.Second // 连接超时
)

func connectBench(tb testing.TB) (*mongo.Database, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), benchConnectTO)
	defer cancel()
	_, db, err := bulkwriter.ConnectMongo(ctx, benchMongoURI, benchDBName)
	if err != nil {
		tb.Skipf("mongo not available (start mongod on :27017): %v", err)
		return nil, false
	}
	return db, true
}

// TestThroughput_1k 写入 1 千条并报告吞吐量。
func TestThroughput_1k(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, ok := connectBench(t)
	if !ok {
		return
	}

	records := makeRecords(1000)
	start := time.Now()
	written, err := bulkwriter.BulkInsert(ctx, db, records)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	rps := float64(written) / elapsed.Seconds()
	fmt.Printf("\n=== 1千条吞吐量测试 ===\n")
	fmt.Printf("写入: %d | 耗时: %v | 吞吐: %.0f rec/s\n", written, elapsed, rps)
}

// TestThroughput_10k 写入 1 万条并报告吞吐量。
func TestThroughput_10k(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, ok := connectBench(t)
	if !ok {
		return
	}

	records := makeRecords(10000)
	start := time.Now()
	written, err := bulkwriter.BulkInsert(ctx, db, records)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	rps := float64(written) / elapsed.Seconds()
	pass := rps >= 10000
	fmt.Printf("\n=== 1万条吞吐量测试 ===\n")
	fmt.Printf("写入: %d | 耗时: %v | 吞吐: %.0f rec/s | 达标(>=1万/s): %v\n",
		written, elapsed, rps, pass)
}

// TestThroughput_100k 写入 10 万条并报告吞吐量。
func TestThroughput_100k(t *testing.T) {
	if testing.Short() {
		t.Skip("skip large test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, ok := connectBench(t)
	if !ok {
		return
	}

	records := makeRecords(100000)
	start := time.Now()
	written, err := bulkwriter.BulkInsert(ctx, db, records)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	rps := float64(written) / elapsed.Seconds()
	fmt.Printf("\n=== 10万条吞吐量测试 ===\n")
	fmt.Printf("写入: %d | 耗时: %v | 吞吐: %.0f rec/s\n", written, elapsed, rps)
}

// TestQueryThroughput 查询吞吐量测试。
func TestQueryThroughput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, ok := connectBench(t)
	if !ok {
		return
	}

	// 先写入 5 万条测试数据
	ops := "bench_query"
	records := make([]bulkwriter.Record, 50000)
	now := time.Now().UnixMilli()
	for i := range records {
		records[i] = bulkwriter.Record{
			Collection: "bench",
			CreatedAt:  now,
			Fields: map[string]interface{}{
				"ops":  ops,
				"psid": fmt.Sprintf("s_%d", i%100),
				"tid":  fmt.Sprintf("t_%d", i),
				"gd":   `{"test":true}`,
			},
		}
	}
	if _, err := bulkwriter.BulkInsert(ctx, db, records); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 测试查询吞吐量
	iterations := 100
	start := time.Now()
	var totalRecords int64

	for i := 0; i < iterations; i++ {
		result, err := bulkwriter.Query(ctx, db, bulkwriter.QueryParams{
			Collection: "bench",
			Ops:        ops,
			Limit:      100,
		})
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		totalRecords += int64(len(result.Records))
	}
	elapsed := time.Since(start)

	qps := float64(iterations) / elapsed.Seconds()
	fmt.Printf("\n=== 查询吞吐量测试 ===\n")
	fmt.Printf("查询: %d次 | 命中: %d条 | 耗时: %v | QPS: %.0f\n",
		iterations, totalRecords, elapsed, qps)
}

// makeRecords 生成 n 条测试记录。
func makeRecords(n int) []bulkwriter.Record {
	records := make([]bulkwriter.Record, n)
	now := time.Now().UnixMilli()
	for i := range records {
		records[i] = bulkwriter.Record{
			Collection: "bench",
			CreatedAt:  now,
			Fields: map[string]interface{}{
				"ops":         "bench",
				"psid":        fmt.Sprintf("s_%d", i%1000),
				"producer_id": i % 10,
				"tba":         float64(i%100) + 0.5,
				"tid":         fmt.Sprintf("t_%d_%d", now, i),
				"twla":        float64(i%90) + 0.5,
				"gd":          fmt.Sprintf(`{"gid":%d,"cc":"VND","msg":"record_%d"}`, i%200, i),
			},
		}
	}
	return records
}
