// Package model 定义生产者和消费者共享的数据结构。
//
// 包含三类结构：
//   - Record: 生产者与消费者之间传递的单条记录（JSON 序列化）
//   - DocRecord: MongoDB 文档结构（BSON 序列化），与 BulkInsert 写入一致
//   - IngestRequest/IngestResponse: HTTP 批量写入的请求/响应
package model

// Record 是生产者和消费者之间传递的单条记录。
//
// 生产者通过 HTTP POST 发送 Record 列表，消费者接收后写入 MongoDB。
// Collection、Ops、PSid 为必填字段，为空会被丢弃。
// Gd 存储业务数据（JSON 字符串格式），bl/tba/twla 等字段可从 Gd JSON 中解析。
type Record struct {
	Collection string  `json:"collection"`  // 目标集合名（必填）
	Ops        string  `json:"ops"`         // 操作标识，用于查询索引（必填）
	PSid       string  `json:"psid"`        // 项目/会话标识，用于查询索引（必填）
	ProducerID int     `json:"producer_id"` // 生产者编号，用于查询索引
	Tba        float64 `json:"tba"`         // 总投注金额
	Tid        string  `json:"tid"`         // 记录唯一ID
	Twla       float64 `json:"twla"`        // 总赢输金额
	Gd         string  `json:"gd"`          // 业务数据（JSON 字符串，包含游戏详情等）
	CreatedAt  int64   `json:"created_at"`  // 创建时间（Unix 毫秒时间戳）
}

// DocRecord 是 MongoDB 中存储的单条文档记录，与 BulkInsert 写入结构一致。
//
// 使用 bson 标签直接映射 MongoDB 文档字段。
// Query 函数返回此结构的切片，可直接访问类型化字段。
type DocRecord struct {
	Ops        string  `bson:"ops"`         // 操作标识
	PSid       string  `bson:"psid"`        // 项目/会话标识
	ProducerID int     `bson:"producer_id"` // 生产者编号
	Tba        float64 `bson:"tba"`         // 总投注金额
	Tid        string  `bson:"tid"`         // 记录唯一ID
	Twla       float64 `bson:"twla"`        // 总赢输金额
	Gd         string  `bson:"gd"`          // 业务数据（JSON 字符串）
	CreatedAt  int64   `bson:"created_at"`  // 创建时间（Unix 毫秒时间戳）
}

// IngestRequest 是消费者接收的批量写入请求。
//
// POST /bulkwriter/ingest 的请求体格式。
type IngestRequest struct {
	Records []Record `json:"records"` // 批量记录列表
}

// IngestResponse 是消费者返回的批量写入结果。
type IngestResponse struct {
	Written int    `json:"written"`        // 已写入数量（异步写入时为 0）
	Error   string `json:"error,omitempty"` // 错误信息
}
