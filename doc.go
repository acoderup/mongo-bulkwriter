// Package bulkwriter 提供高吞吐 MongoDB 异步写入能力。
//
// 分为两端使用：
//   - consumer/ 包：消费者服务端，接收 HTTP 批量数据并写入 MongoDB（本项目使用）
//   - producer/ 包：生产者客户端，供其他服务导入，通过 HTTP 批量发送数据
//
// 两端通过 HTTP 建立稳定连接，可在不同项目中独立部署。
package bulkwriter
