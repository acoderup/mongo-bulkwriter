// Package model 定义生产者和消费者共享的数据结构。
//
// 包含三类结构：
//   - Record: 生产者与消费者之间传递的单条记录（JSON 序列化）
//   - DocRecord: MongoDB 文档结构（BSON 序列化），与 BulkInsert 写入一致
//   - IngestRequest/IngestResponse: HTTP 批量写入的请求/响应
//   - Schema/SchemaRegistry: 记录结构元信息（索引、校验）
package model

import (
	"encoding/json"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Record 通用传输记录。
//
// 基础设施字段（Collection、ProducerID、CreatedAt）为固定顶层字段，
// Fields 包含所有业务字段，由 Schema 定义具体结构。
//
// JSON 兼容两种格式：
//   - 新格式: {"collection":"x","producer_id":1,"created_at":123,"fields":{"ops":"bet",...}}
//   - 旧格式: {"collection":"x","ops":"bet","psid":"y","producer_id":1,...}  自动提取到 Fields
type Record struct {
	Collection string                 `json:"collection"`
	ProducerID int                    `json:"producer_id"`
	CreatedAt  int64                  `json:"created_at"`
	Fields     map[string]interface{} `json:"fields"`
}

// infraKeys 基础设施字段名，旧格式反序列化时从 Fields 中排除。
var infraKeys = map[string]bool{
	"collection":  true,
	"producer_id": true,
	"created_at":  true,
	"fields":      true,
}

// UnmarshalJSON 自定义反序列化，兼容新旧两种 JSON 格式。
//
// 新格式优先走 struct 快速路径（单次解码），旧格式（扁平字段）回退到 map 解析。
func (r *Record) UnmarshalJSON(data []byte) error {
	// 快速路径：新格式 struct 解码
	type fast struct {
		Collection string                 `json:"collection"`
		ProducerID int                    `json:"producer_id"`
		CreatedAt  int64                  `json:"created_at"`
		Fields     map[string]interface{} `json:"fields"`
	}
	var f fast
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	if f.Fields != nil {
		r.Collection = f.Collection
		r.ProducerID = f.ProducerID
		r.CreatedAt = f.CreatedAt
		// 剔除基础设施字段，避免 BSON inline 时与 DocRecord 固定字段冲突
		delete(f.Fields, "producer_id")
		delete(f.Fields, "created_at")
		r.Fields = f.Fields
		return nil
	}

	// 兼容路径：旧格式扁平字段，手动提取
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if c, ok := raw["collection"].(string); ok {
		r.Collection = c
	}
	if v, ok := raw["producer_id"]; ok {
		switch n := v.(type) {
		case float64:
			r.ProducerID = int(n)
		case int:
			r.ProducerID = n
		}
	}
	if v, ok := raw["created_at"]; ok {
		switch n := v.(type) {
		case float64:
			r.CreatedAt = int64(n)
		case int64:
			r.CreatedAt = n
		}
	}

	r.Fields = make(map[string]interface{}, len(raw))
	for k, v := range raw {
		if !infraKeys[k] {
			r.Fields[k] = v
		}
	}
	return nil
}

// DocRecord MongoDB 通用文档结构。
//
// ProducerID、CreatedAt 为固定字段，Fields inline 到文档根级，
// 因此每个业务字段都是独立的 MongoDB 文档字段，可分别建索引。
type DocRecord struct {
	ProducerID int                    `bson:"producer_id"`
	CreatedAt  int64                  `bson:"created_at"`
	Fields     map[string]interface{} `bson:",inline"`
}

// ToDocRecord 将 Record 转为 MongoDB 文档结构。
func (r Record) ToDocRecord() DocRecord {
	// 防御：剔除可能与 DocRecord 固定字段冲突的 key
	if r.Fields != nil {
		delete(r.Fields, "producer_id")
		delete(r.Fields, "created_at")
	}
	return DocRecord{
		ProducerID: r.ProducerID,
		CreatedAt:  r.CreatedAt,
		Fields:     r.Fields,
	}
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

// Schema 定义一种记录结构的元信息：索引策略和校验逻辑。
//
// 不同业务场景对应不同 Schema，各自拥有不同的索引和校验规则。
// 通过 SchemaRegistry 将 Schema 与 MongoDB 集合关联。
type Schema struct {
	Name     string              // 名称，如 "bet"、"payment"
	Indexes  []IndexDefinition   // 该结构需要的索引
	Validate func(r *Record) error // 自定义校验，nil 则仅校验 Collection 非空
}

// IndexDefinition 索引定义，与 mongo.IndexModel 解耦。
type IndexDefinition struct {
	Keys   bson.D // 索引键
	Unique bool   // 唯一索引
}

// ToMongoIndex 转为 mongo.IndexModel。
func (d IndexDefinition) ToMongoIndex() mongo.IndexModel {
	idx := mongo.IndexModel{Keys: d.Keys}
	if d.Unique {
		idx.Options = options.Index().SetUnique(true)
	}
	return idx
}

// SchemaRegistry 管理 collection → Schema 的映射，并发安全。
//
// 未注册的 collection 自动使用默认 Schema。
type SchemaRegistry struct {
	mu       sync.RWMutex
	schemas  map[string]*Schema
	defaults *Schema
}

// NewSchemaRegistry 创建注册表，默认 Schema 为 DefaultSchema()。
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas:  make(map[string]*Schema),
		defaults: DefaultSchema(),
	}
}

// Register 为指定 collection 注册 Schema。
func (r *SchemaRegistry) Register(collection string, s *Schema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[collection] = s
}

// SetDefault 替换默认 Schema，不影响已注册的 collection。
func (r *SchemaRegistry) SetDefault(s *Schema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaults = s
}

// Get 获取指定 collection 的 Schema，未注册则返回默认 Schema。
func (r *SchemaRegistry) Get(collection string) *Schema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if s, ok := r.schemas[collection]; ok {
		return s
	}
	return r.defaults
}

// DefaultRegistry 全局默认注册表，供 bulkwriter/producer/consumer 使用。
var DefaultRegistry = NewSchemaRegistry()

// DefaultSchema 返回当前默认 Schema，包含常用的业务索引。
//
// 索引覆盖：ops, psid, producer_id, tid, created_at 及其复合索引。
// 未注册 Schema 的 collection 使用此默认配置，保证向后兼容。
func DefaultSchema() *Schema {
	return NewSchema("default").
		WithIndex(Asc("ops")).
		WithIndex(Asc("psid")).
		WithIndex(Asc("producer_id")).
		WithIndex(Asc("ops", "psid")).
		WithIndex(Asc("ops", "producer_id")).
		WithIndex(Asc("psid", "producer_id")).
		WithIndex(Asc("ops", "psid", "producer_id")).
		WithIndex(Desc("created_at")).
		WithIndex(Asc("tid")).
		Build()
}

// ──────────────────────────────────────────────
// 索引简写函数（减少 bson.D 样板代码）
// ──────────────────────────────────────────────

// Asc 创建升序索引键。单字段传一个，复合索引传多个。
func Asc(fields ...string) bson.D {
	d := make(bson.D, len(fields))
	for i, f := range fields {
		d[i] = bson.E{Key: f, Value: 1}
	}
	return d
}

// Desc 创建降序索引键。单字段传一个，复合索引传多个。
func Desc(fields ...string) bson.D {
	d := make(bson.D, len(fields))
	for i, f := range fields {
		d[i] = bson.E{Key: f, Value: -1}
	}
	return d
}

// Idx 用 bson.D 键 + 可选的 unique 标记创建 IndexDefinition。
func Idx(keys bson.D, unique ...bool) IndexDefinition {
	return IndexDefinition{
		Keys:   keys,
		Unique: len(unique) > 0 && unique[0],
	}
}

// ──────────────────────────────────────────────
// SchemaBuilder 流式构建器
// ──────────────────────────────────────────────

// SchemaBuilder 提供流式 API 构建 Schema，减少初始化样板。
type SchemaBuilder struct {
	schema *Schema
}

// NewSchema 创建 Schema 构建器。
func NewSchema(name string) *SchemaBuilder {
	return &SchemaBuilder{schema: &Schema{Name: name}}
}

// WithIndex 添加一个索引定义。keys 可用 Asc/Desc 创建，unique 可选。
func (b *SchemaBuilder) WithIndex(keys bson.D, unique ...bool) *SchemaBuilder {
	b.schema.Indexes = append(b.schema.Indexes, IndexDefinition{
		Keys:   keys,
		Unique: len(unique) > 0 && unique[0],
	})
	return b
}

// WithValidate 设置自定义校验函数。
func (b *SchemaBuilder) WithValidate(fn func(r *Record) error) *SchemaBuilder {
	b.schema.Validate = fn
	return b
}

// Build 返回构建好的 Schema。
func (b *SchemaBuilder) Build() *Schema {
	return b.schema
}

// Register 构建 Schema 并注册到默认注册表。
func (b *SchemaBuilder) Register(collection string) *Schema {
	DefaultRegistry.Register(collection, b.schema)
	return b.schema
}

// RegisterSchema 在指定注册表上快速注册 Schema。
// collection 为集合名，name 为 Schema 名称，indexes 为索引列表（用 Idx 创建）。
func (r *SchemaRegistry) RegisterSchema(collection, name string, indexes ...IndexDefinition) *Schema {
	s := &Schema{Name: name, Indexes: indexes}
	r.Register(collection, s)
	return s
}
