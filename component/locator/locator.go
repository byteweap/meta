package locator

import "context"

// Locator 跟踪玩家会话在各节点间的位置
type Locator interface {
	// ID 返回定位器实现标识符
	ID() string

	// AllNodes 返回用户所在所有服务节点
	AllNodes(ctx context.Context, uid int64) (map[string]string, error)

	// Node 返回用户所在某服务节点
	Node(ctx context.Context, uid int64, service string) (string, error)

	// Bind 绑定用户到某服务某节点
	Bind(ctx context.Context, uid int64, service, node string) error

	// UnBind 解绑用户的某服务某节点
	UnBind(ctx context.Context, uid int64, service, node string) error

	// Close 关闭定位器
	Close() error
}

// Session 保存网关的最后一次会话。Node 为空表示该会话已离线。
// ID 是每次连接新生成、不可复用的不透明标识，不代表时间或大小顺序。
type Session struct {
	ID   string
	Node string
}

// SessionLocator 将连接身份与节点位置一起原子维护，供 Gate 和 Mesh 使用。
// 同一服务的会话归属只能通过 Claim/Release 修改，不能混用 Bind/UnBind。
type SessionLocator interface {
	Locator

	// Claim 用新的非空会话 ID 绑定节点，返回是否替换了在线会话。
	// 调用方必须为每次连接生成新 ID；同一用户的并发调用以原子写入顺序为准。
	Claim(ctx context.Context, uid int64, service, node, sessionID string) (bool, error)

	// Release 仅当节点和会话 ID 均匹配且仍在线时解绑，返回是否实际释放。
	// 释放后保留 ID，以便消费方识别迟到的事件。
	Release(ctx context.Context, uid int64, service, node, sessionID string) (bool, error)

	// Session 原子读取最后一次会话及在线节点；不存在时返回零值。
	Session(ctx context.Context, uid int64, service string) (Session, error)
}
