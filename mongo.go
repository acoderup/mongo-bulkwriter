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

// indexedCollections 记录当前进程已确认存在索引的集合（避免重复查询 Mongo）。
var indexedCollections sync.Map

// ConnectMongo 创建高吞吐优化的 MongoDB 连接。
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

// EnsureIndexes 为指定集合显式创建查询索引。幂等操作。
func EnsureIndexes(ctx context.Context, db *mongo.Database, collections ...string) error {
	for _, collName := range collections {
		if err := ensureIndexesFor(ctx, db, collName); err != nil {
			return err
		}
	}
	return nil
}

// ensureIndexesFor 为单个集合创建索引，自动跳过已存在的索引。
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
func BulkInsert(ctx context.Context, db *mongo.Database, records []model.Record) (int, error) {
	groups := make(map[string][]mongo.WriteModel)

	for _, r := range records {
		collName := r.Collection
		if collName == "" {
			collName = "default"
		}
		doc := map[string]interface{}{
			"ops":         r.Ops,
			"psid":        r.PSid,
			"producer_id": r.ProducerID,
			"data":        r.Data,
			"created_at":  r.CreatedAt,
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
type QueryParams struct {
	Collection string // 要查询的集合
	Ops        string // 按 ops 筛选（可选）
	PSid       string // 按 psid 筛选（可选）
	ProducerID int    // 按 producer_id 筛选（可选，0 表示不筛选）
	Limit      int64  // 返回条数限制，默认 100
	Skip       int64  // 跳过条数，用于分页
}

// QueryResult 查询结果。
type QueryResult struct {
	Records []map[string]interface{} `json:"records"`
	Total   int64                    `json:"total"`
}

// Query 根据 ops、psid、producer_id 查询记录。按 created_at 倒序。
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

	coll := db.Collection(params.Collection)

	total, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("count: %w", err)
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}}).
		SetLimit(params.Limit).
		SetSkip(params.Skip)

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}
	defer cursor.Close(ctx)

	var records []map[string]interface{}
	if err := cursor.All(ctx, &records); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	return &QueryResult{Records: records, Total: total}, nil
}
