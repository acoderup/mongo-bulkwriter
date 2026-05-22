// Package bulkwriter 提供高吞吐 MongoDB 异步批量写入、查询和数据管理能力。
//
// 分为四端使用：
//   - 根包：直连 MongoDB，提供 BulkInsert（批量写入）、Query（条件查询）、FindOne（单条查询）、
//     DeleteOldRecords（过期删除）、ListCollections（集合列表）、EnsureIndexes（索引管理）
//   - consumer/ 包：消费者 HTTP 服务端，接收生产者批量数据并异步写入 MongoDB（本项目使用）
//   - producer/ 包：生产者 HTTP 客户端，供其他服务导入，通过 HTTP 批量发送数据
//   - cleanup/ 包：定时清理调度器，按保留月数自动删除过期数据
//
// 生产者与消费者通过 HTTP 解耦，可在不同项目中独立部署。
// 消费者内置 worker 池、并发控制、鉴权、背压保护、自动索引创建。
// 查询支持分页、多条件筛选、时间范围过滤。
// 清理调度器支持可配置的保留期限和执行间隔。
package bulkwriter
