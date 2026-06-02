// Package bulkwriter 提供高吞吐 MongoDB 异步批量写入、查询和数据管理能力。
//
// # 快速开始
//
// 一行注册 Schema（索引 + 校验）：
//
//	bulkwriter.RegisterSchema("pay_logs", "order_id", "user_id", "-created_at")
//
// 批量注册：
//
//	bulkwriter.Configure(
//	    bulkwriter.SchemaConfig{Collection: "bet_logs", Indexes: []string{"ops", "psid", "-created_at"}},
//	    bulkwriter.SchemaConfig{Collection: "pay_logs", Indexes: []string{"order_id", "-created_at"}},
//	)
//
// 快速创建记录：
//
//	bulkwriter.NewRecord("pay_logs", map[string]interface{}{
//	    "order_id": "ORD-001",
//	    "amount":   100.0,
//	})
//
// # 包结构
//
//   - 根包：BulkInsert、Query、FindOne、RegisterSchema、Configure、NewRecord 等
//   - model/：Record、DocRecord、Schema、SchemaRegistry（底层类型）
//   - producer/：生产者 HTTP 客户端，供其他服务导入
//   - consumer/：消费者 HTTP 服务端，接收批量数据异步写入
//   - cleanup/：定时清理调度器
//
// 索引字符串格式："field"=升序，"-field"=降序，"a,b"=复合索引。
package bulkwriter
