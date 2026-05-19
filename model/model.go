// Package model 定义生产者和消费者共享的数据结构。
package model

// Record 是生产者和消费者之间传递的单条记录。
type Record struct {
	Collection string `json:"collection"`  // 目标集合名
	Ops        string `json:"ops"`         // 操作标识，用于查询索引
	Pid        string `json:"pid"`         // 项目/进程标识，用于查询索引
	ProducerID int    `json:"producer_id"` // 生产者编号，用于查询索引
	Data       string `json:"data"`        // 业务数据（字符串，任意格式）
	CreatedAt  int64  `json:"created_at"`  // 创建时间（Unix 毫秒时间戳）
}

// IngestRequest 是消费者接收的批量写入请求。
type IngestRequest struct {
	Records []Record `json:"records"`
}

// IngestResponse 是消费者返回的批量写入结果。
type IngestResponse struct {
	Written int    `json:"written"`
	Error   string `json:"error,omitempty"`
}
