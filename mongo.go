package bulkwriter

import (
	"context"
	"fmt"
	"sync"

	"github.com/acoderup/mongo-bulkwriter/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// indexedCollections 记录当前进程已确认存在索引的集合（进程内缓存，避免重复查询 Mongo）。
var indexedCollections sync.Map

// ConnectMongo 创建高吞吐优化的 MongoDB 连接。
//
// 连接配置：
//   - 最大连接池 300，最小 50（支持高并发写入）
//   - WriteConcern: Unacknowledged（不等待确认，追求极致吞吐）
//   - 禁用重试写入（避免乱序）
func ConnectMongo(ctx context.Context, uri, dbName string) (*mongo.Client, *mongo.Database, error) {
	opts := options.Client().
		ApplyURI(uri).
		SetMaxPoolSize(300).
		SetMinPoolSize(50).
		SetWriteConcern(writeconcern.Unacknowledged()).
		SetRetryWrites(false)

	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("mongo connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, nil, fmt.Errorf("mongo ping: %w", err)
	}

	return client, client.Database(dbName), nil
}

// EnsureIndexes 为指定集合显式创建查询索引。幂等操作，可重复调用。
//
// 首次调用时创建索引，进程内缓存已创建的集合名，后续调用自动跳过。
// 索引列表见 ensureIndexesFor 函数。
func EnsureIndexes(ctx context.Context, db *mongo.Database, collections ...string) error {
	for _, collName := range collections {
		if err := ensureIndexesFor(ctx, db, collName); err != nil {
			return err
		}
	}
	return nil
}

// ensureIndexesFor 为单个集合创建查询索引，自动跳过已存在的索引。
//
// 创建的单字段索引：
//   - ops: 按操作类型查询
//   - psid: 按项目/会话标识查询
//   - producer_id: 按生产者编号查询
//   - tid: 按记录ID查询
//   - created_at: 时间倒序排序
//
// 创建的复合索引：
//   - {ops, psid}
//   - {ops, producer_id}
//   - {psid, producer_id}
//   - {ops, psid, producer_id}
func ensureIndexesFor(ctx context.Context, db *mongo.Database, collName string) error {
	if _, ok := indexedCollections.Load(collName); ok {
		return nil
	}

	coll := db.Collection(collName)

	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list indexes on %s: %w", collName, err)
	}
	defer cursor.Close(ctx)

	var existing []bson.M
	if err := cursor.All(ctx, &existing); err != nil {
		return fmt.Errorf("decode indexes on %s: %w", collName, err)
	}

	hasIndex := make(map[string]bool)
	for _, idx := range existing {
		if key, ok := idx["key"].(bson.D); ok {
			hasIndex[fmt.Sprint(key)] = true
		}
	}

	wanted := []mongo.IndexModel{
		{Keys: bson.D{{Key: "ops", Value: 1}}},
		{Keys: bson.D{{Key: "psid", Value: 1}}},
		{Keys: bson.D{{Key: "producer_id", Value: 1}}},
		{Keys: bson.D{{Key: "ops", Value: 1}, {Key: "psid", Value: 1}}},
		{Keys: bson.D{{Key: "ops", Value: 1}, {Key: "producer_id", Value: 1}}},
		{Keys: bson.D{{Key: "psid", Value: 1}, {Key: "producer_id", Value: 1}}},
		{Keys: bson.D{{Key: "ops", Value: 1}, {Key: "psid", Value: 1}, {Key: "producer_id", Value: 1}}},
		{Keys: bson.D{{Key: "created_at", Value: -1}}},
		{Keys: bson.D{{Key: "tid", Value: 1}}},
	}

	var toCreate []mongo.IndexModel
	for _, idx := range wanted {
		if !hasIndex[fmt.Sprint(idx.Keys)] {
			toCreate = append(toCreate, idx)
		}
	}

	if len(toCreate) > 0 {
		if _, err := coll.Indexes().CreateMany(ctx, toCreate); err != nil {
			return fmt.Errorf("create indexes on %s: %w", collName, err)
		}
	}
	indexedCollections.Store(collName, true)
	return nil
}

// BulkInsert 按 Record.Collection 分组后 unordered BulkWrite 写入 MongoDB。
//
// 特性：
//   - 自动按 Collection 分组写入不同集合
//   - 使用 unordered BulkWrite（单条失败不影响其他）
//   - 首次写入集合时自动创建索引
//   - Collection 为空时默认写入 "default" 集合
//
// 返回值：成功写入的记录数、错误。
func BulkInsert(ctx context.Context, db *mongo.Database, records []model.Record) (int, error) {
	groups := make(map[string][]mongo.WriteModel)

	for _, r := range records {
		collName := r.Collection
		if collName == "" {
			collName = "default"
		}
		doc := model.DocRecord{
			Ops:        r.Ops,
			PSid:       r.PSid,
			ProducerID: r.ProducerID,
			Tba:        r.Tba,
			Tid:        r.Tid,
			Twla:       r.Twla,
			Gd:         r.Gd,
			CreatedAt:  r.CreatedAt,
		}
		groups[collName] = append(groups[collName], mongo.NewInsertOneModel().SetDocument(doc))
	}

	opts := options.BulkWrite().SetOrdered(false)
	total := 0
	for collName, models := range groups {
		if err := ensureIndexesFor(ctx, db, collName); err != nil {
			return total, err
		}

		result, err := db.Collection(collName).BulkWrite(ctx, models, opts)
		if err != nil {
			return total, err
		}
		total += int(result.InsertedCount)
	}
	return total, nil
}

// QueryParams 查询参数。
//
// 所有筛选条件均为可选，未设置的条件不参与筛选。
// 支持时间范围、分页、多条件联合查询。
type QueryParams struct {
	Collection    string // 要查询的集合（必填，为空默认 "default"）
	Ops           string // 按 ops 筛选（可选）
	PSid          string // 按 psid 筛选（可选）
	ProducerID    int    // 按 producer_id 筛选（可选，0 表示不筛选）
	Tid           string // 按 tid 筛选（可选）
	CreatedAfter  int64  // 按 created_at >= 筛选（Unix 毫秒时间戳，可选，0 表示不筛选）
	CreatedBefore int64  // 按 created_at <= 筛选（Unix 毫秒时间戳，可选，0 表示不筛选）
	Limit         int64  // 返回条数限制，默认 100
	Skip          int64  // 跳过条数，用于分页
}

// QueryResult 查询结果。
//
// Records 为按 psid 去重后的 DocRecord 切片（相同 psid 仅保留最新一条），
// Total 为去重后的 psid 总数（不受 Skip/Limit 影响）。
type QueryResult struct {
	Records []model.DocRecord `json:"records"` // 查询结果记录列表（按 psid 去重）
	Total   int64             `json:"total"`   // 去重后的 psid 总数
}

// Query 根据指定条件查询记录，按 created_at 倒序排列。
//
// 结果按 psid 去重：相同 psid 的多条记录只保留 created_at 最新的一条，
// 分页（Limit/Skip）和 Total 均以去重后的 psid 个数为准。
//
// 使用单次聚合查询（$facet）同时获取 total 和 paged records，避免二次往返。
//
// 支持的筛选条件：
//   - ops: 操作类型精确匹配
//   - psid: 项目/会话标识精确匹配
//   - producer_id: 生产者编号精确匹配
//   - tid: 记录ID精确匹配
//   - created_at 时间范围（CreatedAfter <= created_at <= CreatedBefore）
//
// 分页：Limit 默认 100，Skip 用于跳过分页。
// Total 返回符合条件的去重 psid 总数，不受 Limit/Skip 影响。
func Query(ctx context.Context, db *mongo.Database, params QueryParams) (*QueryResult, error) {
	if params.Limit <= 0 {
		params.Limit = 100
	}
	if params.Collection == "" {
		params.Collection = "default"
	}

	filter := bson.M{}
	if params.Ops != "" {
		filter["ops"] = params.Ops
	}
	if params.PSid != "" {
		filter["psid"] = params.PSid
	}
	if params.ProducerID != 0 {
		filter["producer_id"] = params.ProducerID
	}
	if params.Tid != "" {
		filter["tid"] = params.Tid
	}
	if params.CreatedAfter != 0 || params.CreatedBefore != 0 {
		createdFilter := bson.M{}
		if params.CreatedAfter != 0 {
			createdFilter["$gte"] = params.CreatedAfter
		}
		if params.CreatedBefore != 0 {
			createdFilter["$lte"] = params.CreatedBefore
		}
		filter["created_at"] = createdFilter
	}

	coll := db.Collection(params.Collection)

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$sort", Value: bson.D{{Key: "created_at", Value: -1}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$psid"},
			{Key: "doc", Value: bson.M{"$first": "$$ROOT"}},
		}}},
		{{Key: "$replaceRoot", Value: bson.M{"newRoot": "$doc"}}},
		{{Key: "$sort", Value: bson.D{{Key: "created_at", Value: -1}}}},
		{{Key: "$facet", Value: bson.D{
			{Key: "metadata", Value: mongo.Pipeline{
				{{Key: "$count", Value: "total"}},
			}},
			{Key: "records", Value: mongo.Pipeline{
				{{Key: "$skip", Value: params.Skip}},
				{{Key: "$limit", Value: params.Limit}},
			}},
		}}},
	}

	aggOpts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := coll.Aggregate(ctx, pipeline, aggOpts)
	if err != nil {
		return nil, fmt.Errorf("aggregate: %w", err)
	}
	defer cursor.Close(ctx)

	var facetResults []struct {
		Metadata []struct {
			Total int64 `bson:"total"`
		} `bson:"metadata"`
		Records []model.DocRecord `bson:"records"`
	}
	if err := cursor.All(ctx, &facetResults); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	total := int64(0)
	records := []model.DocRecord(nil)
	if len(facetResults) > 0 {
		if len(facetResults[0].Metadata) > 0 {
			total = facetResults[0].Metadata[0].Total
		}
		records = facetResults[0].Records
	}

	return &QueryResult{Records: records, Total: total}, nil
}
