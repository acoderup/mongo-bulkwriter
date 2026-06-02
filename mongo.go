package bulkwriter

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/acoderup/mongo-bulkwriter/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Record 类型别名，方便用户直接使用 bulkwriter.Record 而无需导入 model 包。
type Record = model.Record

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

// Reconnect 断开旧连接并尝试重新连接 MongoDB。
// 最多重试 maxAttempts 次，每次超时 timeout。全部失败时返回最后一次错误。
func Reconnect(ctx context.Context, oldClient *mongo.Client, uri, dbName string, maxAttempts int, timeout time.Duration) (*mongo.Client, *mongo.Database, error) {
	// 断开旧连接，忽略错误
	if oldClient != nil {
		oldClient.Disconnect(context.Background())
	}

	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			log.Printf("[bulkwriter] 重连 Mongo 第 %d/%d 次...", i+1, maxAttempts)
		}

		pingCtx, cancel := context.WithTimeout(ctx, timeout)
		client, db, err := ConnectMongo(pingCtx, uri, dbName)
		cancel()

		if err == nil {
			log.Println("[bulkwriter] Mongo 重连成功")
			return client, db, nil
		}
		lastErr = err
	}
	return nil, nil, fmt.Errorf("重连失败 (%d 次): %w", maxAttempts, lastErr)
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

// ensureIndexesFor 为单个集合按注册的 Schema 创建查询索引，自动跳过已存在的索引。
//
// 索引列表由 SchemaRegistry 中的 Schema.Indexes 决定，未注册则使用默认 Schema。
// 首次调用时创建索引，进程内缓存已创建的集合名，后续调用自动跳过。
func ensureIndexesFor(ctx context.Context, db *mongo.Database, collName string) error {
	// 快速路径：已确认存在索引
	if _, ok := indexedCollections.Load(collName); ok {
		return nil
	}

	// 注：以下 Load→CreateMany→Store 之间存在竞态窗口，
	// 并发首次写入时可能多个 goroutine 同时执行 CreateMany。
	// CreateMany 对已存在索引是幂等操作，仅浪费一次 List 调用，不影响正确性。

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

	// 从 Registry 获取 Schema 的索引定义
	schema := model.DefaultRegistry.Get(collName)
	wanted := make([]mongo.IndexModel, 0, len(schema.Indexes))
	for _, def := range schema.Indexes {
		wanted = append(wanted, def.ToMongoIndex())
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
		groups[collName] = append(groups[collName],
			mongo.NewInsertOneModel().SetDocument(r.ToDocRecord()))
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
// 便捷字段（Ops/PSid 等）与 Filter 同时生效（AND 逻辑）。
// 分页以 psid 去重个数为准，适用于投注类按 psid 分组的场景。
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
	Filter        bson.M // 自定义筛选条件，与便捷字段合并（可选）
}

// QueryResult 查询结果。
//
// Records 为当前页 psid 分组内的全部文档（可能超过 Limit 条），
// Total 为去重后的 psid 总数（不受 Skip/Limit 影响）。
type QueryResult struct {
	Records []model.DocRecord `json:"records"` // 当前页全部记录
	Total   int64             `json:"total"`   // 去重后的 psid 总数
}

// Query 根据指定条件查询记录，按 psid 分组分页。
//
// 分页以 psid 去重个数为准：相同 psid 的多条记录视为一组。
// Total 为去重后的 psid 总数，Skip/Limit 按 psid 分组维度截取，
// 但每组内的全部文档均会返回（返回条数可能超过 Limit）。
//
// 使用两次查询：聚合获取分页 psid 列表 + Find 获取对应全部文档。
//
// 支持的筛选条件：
//   - ops: 操作类型精确匹配
//   - psid: 项目/会话标识精确匹配
//   - producer_id: 生产者编号精确匹配
//   - tid: 记录ID精确匹配
//   - created_at 时间范围（CreatedAfter <= created_at <= CreatedBefore）
//
// 分页：Limit 默认 100（分组数），Skip 用于跳过指定组数。
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
	// 合并自定义 Filter（与便捷字段 AND 逻辑）
	for k, v := range params.Filter {
		filter[k] = v
	}

	coll := db.Collection(params.Collection)

	psidPipeline := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$psid"},
			{Key: "max_created", Value: bson.M{"$max": "$created_at"}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "max_created", Value: -1}}}},
		{{Key: "$facet", Value: bson.D{
			{Key: "metadata", Value: mongo.Pipeline{
				{{Key: "$count", Value: "total"}},
			}},
			{Key: "paged", Value: mongo.Pipeline{
				{{Key: "$skip", Value: params.Skip}},
				{{Key: "$limit", Value: params.Limit}},
			}},
		}}},
	}

	cursor, err := coll.Aggregate(ctx, psidPipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate psids: %w", err)
	}
	defer cursor.Close(ctx)

	var facetResults []struct {
		Metadata []struct {
			Total int64 `bson:"total"`
		} `bson:"metadata"`
		Paged []struct {
			PSid string `bson:"_id"`
		} `bson:"paged"`
	}
	if err := cursor.All(ctx, &facetResults); err != nil {
		return nil, fmt.Errorf("decode facet: %w", err)
	}

	total := int64(0)
	var pagedPsids []string
	if len(facetResults) > 0 {
		if len(facetResults[0].Metadata) > 0 {
			total = facetResults[0].Metadata[0].Total
		}
		pagedPsids = make([]string, 0, len(facetResults[0].Paged))
		for _, p := range facetResults[0].Paged {
			pagedPsids = append(pagedPsids, p.PSid)
		}
	}

	if total == 0 || len(pagedPsids) == 0 {
		return &QueryResult{Records: nil, Total: total}, nil
	}

	findFilter := bson.M{"psid": bson.M{"$in": pagedPsids}}
	findOpts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}})

	docsCursor, err := coll.Find(ctx, findFilter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}
	defer docsCursor.Close(ctx)

	var records []model.DocRecord
	if err := docsCursor.All(ctx, &records); err != nil {
		return nil, fmt.Errorf("decode records: %w", err)
	}

	return &QueryResult{Records: records, Total: total}, nil
}

// FindOne 精确查询单条记录，适用于按 tid 等唯一键查询的场景。
//
// 直接使用 collection.FindOne，无聚合、无分组、无分页，性能最高。
// 未找到时返回 nil, nil（非错误）。
func FindOne(ctx context.Context, db *mongo.Database, collection string, filter bson.M) (*model.DocRecord, error) {
	if collection == "" {
		collection = "default"
	}

	var doc model.DocRecord
	err := db.Collection(collection).FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("findOne: %w", err)
	}
	return &doc, nil
}

// DeleteOldRecords 分批删除 collection 中 created_at < before 的记录。
//
// 适用于千万级数据：每批取 batchSize 条 _id，再按 _id 精确删除，
// 避免单次 DeleteMany 长时间锁库。批次间检查 ctx.Done() 支持取消。
// created_at 字段必须有索引。
func DeleteOldRecords(ctx context.Context, db *mongo.Database, collection string, before int64) (int64, error) {
	const batchSize = 10000

	if collection == "" {
		collection = "default"
	}

	coll := db.Collection(collection)
	filter := bson.M{"created_at": bson.M{"$lt": before}}
	findOpts := options.Find().
		SetLimit(batchSize).
		SetProjection(bson.M{"_id": 1})
	var total int64
	batchNum := 0

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}

		cursor, err := coll.Find(ctx, filter, findOpts)
		if err != nil {
			return total, fmt.Errorf("find batch: %w", err)
		}

		ids := make([]any, 0, batchSize)
		for cursor.Next(ctx) {
			var doc struct {
				ID any `bson:"_id"`
			}
			if err := cursor.Decode(&doc); err != nil {
				cursor.Close(ctx)
				return total, fmt.Errorf("decode id: %w", err)
			}
			ids = append(ids, doc.ID)
		}
		cursor.Close(ctx)

		if len(ids) == 0 {
			break
		}

		result, err := coll.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
		if err != nil {
			return total, fmt.Errorf("delete batch: %w", err)
		}
		total += result.DeletedCount
		batchNum++

		if batchNum%10 == 0 {
			log.Printf("[bulkwriter] %s: 已删除 %d 条 (%d 批次)", collection, total, batchNum)
		}
	}

	return total, nil
}

// ListCollections 返回数据库中所有集合名称。
func ListCollections(ctx context.Context, db *mongo.Database) ([]string, error) {
	return db.ListCollectionNames(ctx, bson.M{})
}

// ──────────────────────────────────────────────
// 顶层封装：一个函数完成 Schema 注册
// ──────────────────────────────────────────────

// parseIndexes 将字符串索引转为 IndexDefinition。
// 格式："field"=升序，"-field"=降序，"a,b"=复合升序，"-a,-b"=复合降序。
func parseIndexes(indexes []string) []model.IndexDefinition {
	defs := make([]model.IndexDefinition, 0, len(indexes))
	for _, idx := range indexes {
		parts := strings.Split(idx, ",")
		desc := strings.HasPrefix(parts[0], "-")
		fields := make([]string, len(parts))
		for i, p := range parts {
			fields[i] = strings.TrimPrefix(p, "-")
		}
		if desc {
			defs = append(defs, model.Idx(model.Desc(fields...)))
		} else {
			defs = append(defs, model.Idx(model.Asc(fields...)))
		}
	}
	return defs
}

// SchemaConfig 定义单个集合的 Schema 配置。
type SchemaConfig struct {
	Collection string                      // 集合名
	Indexes    []string                    // 索引字段："field"=升序，"-field"=降序
	Validate   func(r *model.Record) error  // 可选校验
}

// RegisterSchema 为集合注册 Schema，只需传入索引字段名。
//
// 格式： "field" = 升序索引，"-field" = 降序索引，"a,b" = 复合索引。
//
//	bulkwriter.RegisterSchema("pay_logs", "order_id", "user_id", "-created_at")
func RegisterSchema(collection string, indexes ...string) *model.Schema {
	s := &model.Schema{
		Name:    collection,
		Indexes: parseIndexes(indexes),
	}
	model.DefaultRegistry.Register(collection, s)
	return s
}

// Configure 批量注册 Schema 配置。
//
//	bulkwriter.Configure(
//	    bulkwriter.SchemaConfig{Collection: "bet_logs", Indexes: []string{"ops", "psid", "-created_at"}},
//	    bulkwriter.SchemaConfig{Collection: "pay_logs", Indexes: []string{"order_id", "-created_at"}},
//	)
func Configure(schemas ...SchemaConfig) {
	for _, sc := range schemas {
		s := &model.Schema{
			Name:     sc.Collection,
			Indexes:  parseIndexes(sc.Indexes),
			Validate: sc.Validate,
		}
		model.DefaultRegistry.Register(sc.Collection, s)
	}
}

// NewRecord 快速创建一条记录。
func NewRecord(collection string, fields map[string]interface{}) model.Record {
	return model.Record{
		Collection: collection,
		CreatedAt:  time.Now().UnixMilli(),
		Fields:     fields,
	}
}

// NewRecordWithTime 快速创建带自定义时间的记录。
func NewRecordWithTime(collection string, createdAt int64, fields map[string]interface{}) model.Record {
	return model.Record{
		Collection: collection,
		CreatedAt:  createdAt,
		Fields:     fields,
	}
}
