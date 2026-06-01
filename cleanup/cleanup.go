// Package cleanup 提供定时清理 MongoDB 过期数据的调度器。
//
// 启动后按配置的间隔遍历所有集合，删除超过保留天数的记录（基于 created_at 字段）。
//
// 用法：
//
//	stop := cleanup.Start(ctx, db, cleanup.Config{RetentionDays: 100, IntervalHours: 72})
//	defer stop()
package cleanup

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/acoderup/mongo-bulkwriter"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Config 清理配置。零值字段使用默认值。
type Config struct {
	RetentionDays int // 保留天数，默认 100
	IntervalHours int // 执行间隔（小时），默认 72（3 天）
}

func (c *Config) withDefaults() {
	if c.RetentionDays <= 0 {
		c.RetentionDays = 100
	}
	if c.IntervalHours <= 0 {
		c.IntervalHours = 72
	}
}

// Start 启动后台清理 goroutine，返回 stop 函数用于优雅关闭。
// stop 会等待当前正在执行的清理任务完成后再返回（最多 2 小时）。
func Start(ctx context.Context, db *mongo.Database, cfg Config) (stop func()) {
	cfg.withDefaults()

	done := make(chan struct{})
	ticker := time.NewTicker(time.Duration(cfg.IntervalHours) * time.Hour)

	var taskCtx context.Context
	var taskCancel context.CancelFunc

	go func() {
		defer ticker.Stop()
		defer close(done)

		log.Printf("[cleanup] 已启动: 保留 %d 天, 间隔 %d 小时",
			cfg.RetentionDays, cfg.IntervalHours)

		for {
			select {
			case <-ctx.Done():
				log.Println("[cleanup] 收到退出信号")
				if taskCancel != nil {
					taskCancel()
				}
				return
			case <-ticker.C:
				taskCtx, taskCancel = context.WithTimeout(ctx, 2*time.Hour)
				run(taskCtx, db, cfg.RetentionDays)
				taskCancel()
			}
		}
	}()

	return func() {
		ticker.Stop()
		<-done
	}
}

func run(ctx context.Context, db *mongo.Database, retentionDays int) {
	cutoff := time.Now().AddDate(0, 0, -retentionDays).UnixMilli()
	log.Printf("[cleanup] 开始清理 created_at < %d (%s 之前)",
		cutoff, time.UnixMilli(cutoff).Format("2006-01-02"))

	collections, err := bulkwriter.ListCollections(ctx, db)
	if err != nil {
		log.Printf("[cleanup] 获取集合列表失败: %v", err)
		return
	}

	var totalDeleted int64
	for _, col := range collections {
		if strings.HasPrefix(col, "system.") {
			continue
		}

		select {
		case <-ctx.Done():
			log.Printf("[cleanup] 清理被中断，已删 %d 条", totalDeleted)
			return
		default:
		}

		log.Printf("[cleanup] 正在清理 %s ...", col)
		deleted, err := bulkwriter.DeleteOldRecords(ctx, db, col, cutoff)
		if err != nil {
			log.Printf("[cleanup] 清理 %s 失败: %v", col, err)
			continue
		}
			if deleted > 0 {
				log.Printf("[cleanup] %s: 删除 %d 条", col, deleted)
			}
			totalDeleted += deleted
	}

	log.Printf("[cleanup] 清理完成，共删除 %d 条记录", totalDeleted)
}
