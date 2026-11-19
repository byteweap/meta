# Redis Locator

本包实现 `component/locator.SessionLocator`，使用 Hash 保存用户在不同服务上的节点归属及网关会话身份。

**特性**
- 基于单个 Redis Hash 原子维护节点与会话，不涉及多键事务或 Redis Cluster 跨槽操作
- 支持统一前缀与默认键规则
- 普通业务节点使用 `Bind/UnBind`；网关连接使用 `Claim/Release/Session`

**安装**

```bash
go get github.com/byteweap/meta/contrib/locator/redis
```

**最小用法**

```go
package main

import (
  "context"
  "crypto/rand"
  "time"

  rloc "github.com/byteweap/meta/contrib/locator/redis"
  "github.com/redis/go-redis/v9"
)

func main() {
  opts := redis.UniversalOptions{
    Addrs: []string{"127.0.0.1:6379"},
  }

  loc := rloc.New(opts, "wk")
  defer loc.Close()

  ctx := context.Background()
  runID := rand.Text() // 每次 Gate 启动生成，不可复用
  _ = loc.AcquireGateLease(ctx, runID, time.Minute)
  sessionID := rand.Text() // 每次连接新生成，不可复用
  replaced, _ := loc.Claim(ctx, 1001, "gate", "node-1", sessionID, runID)
  _ = replaced // true 表示替换了尚未释放的节点记录

  state, _ := loc.Session(ctx, 1001, "gate")
  _ = state // Node 为空表示离线

  released, _ := loc.Release(ctx, 1001, "gate", "node-1", sessionID)
  _ = released // 仅 true 时可发本次会话的下线事件
}
```

**配置**
- `New(opts, prefix)` 使用 `redis.UniversalOptions`
- 构造时强制关闭 `ReadOnly`、`RouteByLatency` 和 `RouteRandomly`，保证 Cluster/Sentinel 的归属查询读取主节点；副本延迟可能让旧事件被错误接受。
- `prefix` 为空时使用默认键规则

**注意事项**
- 键规则：`<prefix>:locator:<uid>`，当 `prefix` 为空时为 `locator:<uid>`
- 用户 Hash 的 `node:<service>` 保存节点，`session:<service>` 保存最后一个会话 ID，`run:<service>` 保存拥有该会话的 Gate 运行 ID。`Node/AllNodes` 直接读取位置字段。
- Gate 运行租约键为 `<prefix>:gate-lease:<runID>`，空前缀时省略前缀段。Redis 键 TTL 负责到期，续期只延长现有的键。
- `Claim` 先读取已有会话 ID，再检查租约，最后在用户 Hash 中比较 ID 并原子更新。如果期间归属已变更，会重新读取并验证租约。`Release` 比较节点与会话 ID 后移除节点和运行 ID，保留最后的会话 ID。
- `Session` 读取归属后检查对应租约，租约缺失时返回 ID 和空节点。两个键可位于不同 Redis Cluster 槽位，分别执行单键命令。旧格式会话缺少运行 ID 时按离线查询，重新连接后写入新归属。
- `Bind/UnBind` 用于业务节点定位。`UnBind` 原子比较节点后删除。相同服务一旦使用会话接口，普通绑定和解绑会返回错误，避免绕过会话归属校验。
- `service`、写操作的 `node` 和会话标识必须非空。所有操作均传递调用方的上下文和 Redis 错误。
- 定位器启用 go-redis 的 `ContextTimeoutEnabled`。使用套接字期限时，网络读写取请求期限与配置超时中的较早值。显式禁用底层套接字期限时，遵循传入的读写配置。主动取消后，已经阻塞的读取可能持续到收到响应或读超时。
- 此版本直接采用新字段结构，不读取历史 `service -> node` 字段；部署前须清理旧位置数据并重建在线归属。

**验证**

`go test -mod=readonly ./...` 和 `go test -mod=readonly -race ./...` 使用 miniredis 验证租约到期、同节点和跨节点重连、并发占用与释放、离线记录、路由投影及失败路径。主从槽位模拟覆盖主节点读取。真实 Redis Cluster 联调单独进行。

