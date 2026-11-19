// Package redis 使用 Redis 作为后端实现定位器插件
package redis

import (
	"context"
	"errors"
	"strings"
	"time"

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
if (redis.call('HGET', KEYS[1], ARGV[2]) or '') ~= ARGV[7] then
  return -1
end
local replaced = redis.call('HEXISTS', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4], ARGV[2], ARGV[5], ARGV[3], ARGV[6])
return replaced
`)

var releaseScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[4]
   and redis.call('HGET', KEYS[1], ARGV[2]) == ARGV[5] then
  redis.call('HDEL', KEYS[1], ARGV[1], ARGV[3])
  return 1
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

// AllNodes 返回位置记录。检查 Gate 会话是否在线使用 Session
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

// Node 返回位置记录，找不到时返回空字符串。检查 Gate 会话是否在线使用 Session
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

// Claim 校验 Gate 租约后原子写入会话归属，返回是否替换了尚未释放的节点记录
func (l *Locator) Claim(ctx context.Context, uid int64, service, node, sessionID, runID string) (bool, error) {
	if err := validateSession(service, node, sessionID); err != nil {
		return false, err
	}
	if runID == "" {
		return false, errors.New("gate run ID must not be empty")
	}
	key := l.key(uid)
	nodeField, sessionField, runField := "node:"+service, "session:"+service, "run:"+service
	for ctx.Err() == nil {
		// 先读取会话 ID。租约校验期间发生重连时，写入前必须再校验租约
		previousID, err := normalizeNodeResult(l.rc.HGet(ctx, key, sessionField).Result())
		if err != nil {
			return false, err
		}
		alive, err := l.gateLeaseActive(ctx, runID)
		if err != nil {
			return false, err
		}
		if !alive {
			return false, errors.New("gate lease is not active")
		}
		result, err := claimScript.Run(ctx, l.rc, []string{key}, nodeField, sessionField, runField, node, sessionID, runID, previousID).Int()
		if err != nil || result >= 0 {
			return result == 1, err
		}
	}
	return false, ctx.Err()
}

// Release 仅释放节点和 ID 都匹配的会话，保留最后的会话 ID
func (l *Locator) Release(ctx context.Context, uid int64, service, node, sessionID string) (bool, error) {
	if err := validateSession(service, node, sessionID); err != nil {
		return false, err
	}
	result, err := releaseScript.Run(ctx, l.rc, []string{l.key(uid)}, "node:"+service, "session:"+service, "run:"+service, node, sessionID).Int()
	return result == 1, err
}

// Session 读取最后的会话和 Gate 租约。已释放或已失效时返回会话 ID，节点为空
func (l *Locator) Session(ctx context.Context, uid int64, service string) (locator.Session, error) {
	if service == "" {
		return locator.Session{}, errors.New("service must not be empty")
	}
	values, err := l.rc.HMGet(ctx, l.key(uid), "session:"+service, "node:"+service, "run:"+service).Result()
	if err != nil {
		return locator.Session{}, err
	}
	id, _ := values[0].(string)
	node, _ := values[1].(string)
	runID, _ := values[2].(string)
	if id == "" {
		return locator.Session{}, nil
	}
	session := locator.Session{ID: id}
	if node == "" || runID == "" {
		return session, nil
	}
	alive, err := l.gateLeaseActive(ctx, runID)
	if err != nil {
		return locator.Session{}, err
	}
	if alive {
		session.Node = node
	}
	return session, nil
}

// AcquireGateLease 建立 Gate 运行租约，运行 ID 已存在时返回错误。ttl 至少为 1 毫秒
func (l *Locator) AcquireGateLease(ctx context.Context, runID string, ttl time.Duration) error {
	if err := validateGateLease(runID, ttl); err != nil {
		return err
	}
	created, err := l.rc.SetNX(ctx, l.gateLeaseKey(runID), "1", ttl).Result()
	if err != nil {
		return err
	}
	if !created {
		return errors.New("gate lease already exists")
	}
	return nil
}

// RenewGateLease 延长原生键 TTL。租约已失效时返回 false，ttl 至少为 1 毫秒
func (l *Locator) RenewGateLease(ctx context.Context, runID string, ttl time.Duration) (bool, error) {
	if err := validateGateLease(runID, ttl); err != nil {
		return false, err
	}
	return l.rc.PExpire(ctx, l.gateLeaseKey(runID), ttl).Result()
}

func (l *Locator) gateLeaseActive(ctx context.Context, runID string) (bool, error) {
	count, err := l.rc.Exists(ctx, l.gateLeaseKey(runID)).Result()
	return count != 0, err
}

func (l *Locator) gateLeaseKey(runID string) string {
	if l.prefix == "" {
		return "gate-lease:" + runID
	}
	return l.prefix + ":gate-lease:" + runID
}

func validateGateLease(runID string, ttl time.Duration) error {
	if runID == "" {
		return errors.New("gate run ID must not be empty")
	}
	if ttl < time.Millisecond {
		return errors.New("gate lease TTL must be at least 1 ms")
	}
	return nil
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
