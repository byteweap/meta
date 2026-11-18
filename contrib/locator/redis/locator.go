// Package redis 使用 Redis 作为后端实现定位器插件
package redis

import (
	"context"
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/byteweap/meta/component/locator"
	"github.com/byteweap/meta/pkg/conv"
)

// ID Redis 定位器实现标识符
const ID = "redis(hash)"

// Locator 使用单个用户 Hash 原子维护节点位置与会话身份。
type Locator struct {
	rc     redis.UniversalClient // Redis 客户端，用于哈希操作
	prefix string
}

var _ locator.SessionLocator = (*Locator)(nil)

var bindScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
  return redis.error_reply('session-managed service requires Claim/Release')
end
return redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
`)

var unbindScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
  return redis.error_reply('session-managed service requires Claim/Release')
end
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[3] then
  return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
`)

var claimScript = redis.NewScript(`
local replaced = redis.call('HEXISTS', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3], ARGV[2], ARGV[4])
return replaced
`)

var releaseScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[3]
   and redis.call('HGET', KEYS[1], ARGV[2]) == ARGV[4] then
  return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
`)

// New 创建从主节点读取的 Redis 定位器。启用套接字期限时，网络读写取调用方与配置中较早的截止时间
func New(opts redis.UniversalOptions, prefix string) *Locator {
	opts.ContextTimeoutEnabled = true
	// 会话归属必须从主节点读取，避免副本延迟让已失效的会话事件通过校验
	opts.ReadOnly = false
	opts.RouteByLatency = false
	opts.RouteRandomly = false
	return newWith(redis.NewUniversalClient(&opts), prefix)
}

// newWith 使用 Redis 客户端创建 Redis 定位器
func newWith(rc redis.UniversalClient, prefix string) *Locator {
	return &Locator{
		rc:     rc,
		prefix: prefix,
	}
}

// key 生成 Redis 键
func (l *Locator) key(uid int64) string {
	if l.prefix == "" {
		return "locator:" + conv.String(uid)
	}
	return l.prefix + ":locator:" + conv.String(uid)
}

// ID 返回定位器实现标识符
func (l *Locator) ID() string {
	return ID
}

// AllNodes 只返回在线节点，不暴露会话标识或离线记录。
func (l *Locator) AllNodes(ctx context.Context, uid int64) (map[string]string, error) {
	fields, err := l.rc.HGetAll(ctx, l.key(uid)).Result()
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]string)
	for field, node := range fields {
		if service, ok := strings.CutPrefix(field, "node:"); ok {
			nodes[service] = node
		}
	}
	return nodes, nil
}

// Node 返回用户ID当前所在的某服务某节点
func (l *Locator) Node(ctx context.Context, uid int64, service string) (string, error) {
	if service == "" {
		return "", errors.New("service must not be empty")
	}
	return normalizeNodeResult(l.rc.HGet(ctx, l.key(uid), "node:"+service).Result())
}

func normalizeNodeResult(node string, err error) (string, error) {
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return node, err
}

// Bind 绑定用户ID到某服务某节点
func (l *Locator) Bind(ctx context.Context, uid int64, service, node string) error {
	if err := validateBinding(service, node); err != nil {
		return err
	}
	return bindScript.Run(ctx, l.rc, []string{l.key(uid)}, "node:"+service, "session:"+service, node).Err()
}

// UnBind 如果节点匹配则解绑用户的某服务某节点
func (l *Locator) UnBind(ctx context.Context, uid int64, service, node string) error {
	if err := validateBinding(service, node); err != nil {
		return err
	}
	return unbindScript.Run(ctx, l.rc, []string{l.key(uid)}, "node:"+service, "session:"+service, node).Err()
}

// Claim 原子替换节点和会话标识，返回是否接替了在线会话。
func (l *Locator) Claim(ctx context.Context, uid int64, service, node, sessionID string) (bool, error) {
	if err := validateSession(service, node, sessionID); err != nil {
		return false, err
	}
	result, err := claimScript.Run(ctx, l.rc, []string{l.key(uid)}, "node:"+service, "session:"+service, node, sessionID).Int()
	return result == 1, err
}

// Release 仅释放当前在线会话，保留最后一个标识用于过滤迟到事件。
func (l *Locator) Release(ctx context.Context, uid int64, service, node, sessionID string) (bool, error) {
	if err := validateSession(service, node, sessionID); err != nil {
		return false, err
	}
	result, err := releaseScript.Run(ctx, l.rc, []string{l.key(uid)}, "node:"+service, "session:"+service, node, sessionID).Int()
	return result == 1, err
}

// Session 在同一条命令中读取节点和最后一个会话标识。
func (l *Locator) Session(ctx context.Context, uid int64, service string) (locator.Session, error) {
	if service == "" {
		return locator.Session{}, errors.New("service must not be empty")
	}
	values, err := l.rc.HMGet(ctx, l.key(uid), "session:"+service, "node:"+service).Result()
	if err != nil {
		return locator.Session{}, err
	}
	if values[0] == nil {
		return locator.Session{}, nil
	}
	session := locator.Session{ID: values[0].(string)}
	if values[1] != nil {
		session.Node = values[1].(string)
	}
	return session, nil
}

func validateBinding(service, node string) error {
	if service == "" || node == "" {
		return errors.New("service and node must not be empty")
	}
	return nil
}

func validateSession(service, node, sessionID string) error {
	if err := validateBinding(service, node); err != nil {
		return err
	}
	if sessionID == "" {
		return errors.New("session ID must not be empty")
	}
	return nil
}

// Close 关闭定位器
func (l *Locator) Close() error {
	return l.rc.Close()
}
