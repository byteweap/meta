package locator

import (
	"context"
	"time"
)

// Locator 跟踪玩家会话在各节点间的位置
type Locator interface {
	// ID 返回定位器实现标识符
	ID() string

	// AllNodes 返回用户记录的所有服务节点
	AllNodes(ctx context.Context, uid int64) (map[string]string, error)

	// Node 返回用户在指定服务记录的节点
	Node(ctx context.Context, uid int64, service string) (string, error)

	// Bind 绑定用户到某服务某节点
	Bind(ctx context.Context, uid int64, service, node string) error

	// UnBind 解绑用户的某服务某节点
	UnBind(ctx context.Context, uid int64, service, node string) error

	// Close 关闭定位器
	Close() error
}

// Session 保存网关的最后一次会话。Node 为空表示该会话已离线或租约已到期
// ID 是每次连接新生成、不可复用的不透明标识，不代表时间或大小顺序
type Session struct {
	ID   string
	Node string
}

// SessionLocator 维护 Gate 的运行租约和连接归属，供 Gate 和 Mesh 使用
// 同一服务不能混用会话操作和 Bind/UnBind。判断 Gate 会话在线状态时使用 Session
type SessionLocator interface {
	Locator

	// AcquireGateLease 为全新的非空运行 ID 建立租约。ttl 至少为 1 毫秒，运行 ID 不可复用
	AcquireGateLease(ctx context.Context, runID string, ttl time.Duration) error

	// RenewGateLease 延长运行租约。ttl 至少为 1 毫秒，租约已失效时返回 false 且不重新建立
	RenewGateLease(ctx context.Context, runID string, ttl time.Duration) (bool, error)

	// Claim 将新的非空会话 ID 与 Gate 运行 ID 原子写入，返回是否替换了尚未释放的节点记录
	// 写入前检查运行租约。同一用户的并发认领以归属写入顺序为准
	Claim(ctx context.Context, uid int64, service, node, sessionID, runID string) (bool, error)

	// Release 仅当节点和会话 ID 均匹配时解绑，返回是否实际释放
	// 释放后保留 ID，以便消费方识别迟到的事件
	Release(ctx context.Context, uid int64, service, node, sessionID string) (bool, error)

	// Session 读取最后一次会话，并检查其 Gate 运行租约。已释放或租约失效时节点为空
	// 会话不存在时返回零值。读取期间的重连可能使结果只反映此前的归属
	Session(ctx context.Context, uid int64, service string) (Session, error)
}
