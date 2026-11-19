// Package observe 定义 Gate 和 Mesh 的请求观测接口
package observe

import "time"

// Stage 表示框架请求路径中的计时阶段
type Stage string

const (
	// Routing 统计 Gate 查找服务和选择目标实例的时间
	Routing Stage = "routing"
	// Queue 统计 Mesh 从尝试入队至出队处理的时间
	Queue Stage = "queue"
	// Handler 统计 Mesh 执行业务 handler 的时间
	Handler Stage = "handler"
	// Reply 统计 Mesh 向 Broker 提交回包的时间
	Reply Stage = "reply"
)

// Reason 表示框架拒绝请求的原因
type Reason string

const (
	// Unavailable 表示 Gate 无法选定目标或向目标发布消息
	Unavailable Reason = "unavailable"
	// QueueFull 表示 Mesh 的 RPC 执行与排队名额已经用尽
	QueueFull Reason = "queue_full"
	// DeadlineExceeded 表示 Mesh 判定 RPC 执行预算已耗尽
	DeadlineExceeded Reason = "deadline_exceeded"
	// Stopping 表示服务停止接收新请求，或排空终止时丢弃尚未执行的请求
	Stopping Reason = "stopping"
)

// Observer 接收请求耗时和框架拒绝事件。调用发生在原请求路径上且可能并发
// 实现应支持并发、快速返回，并避免阻塞或重新进入服务。框架会记录回调的 panic 并继续处理请求
type Observer interface {
	// Observe 记录一次已执行阶段的耗时
	Observe(stage Stage, elapsed time.Duration)
	// Reject 记录一次框架决策。业务 handler 自行返回的状态码不会触发它
	Reject(reason Reason)
}
