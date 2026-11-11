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
  sessionID := rand.Text() // 每次连接新生成，不可复用
  replaced, _ := loc.Claim(ctx, 1001, "gate", "node-1", sessionID)
  _ = replaced // true 表示接替了在线会话，可发重连事件

  node, _ := loc.Node(ctx, 1001, "gate")
  _ = node

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
- Hash 字段 `node:<service>` 保存在线节点，`session:<service>` 保存最后一个会话标识。`Node/AllNodes` 只返回节点字段，`Session` 一次读取标识与节点；普通 `Bind` 没有会话标识，`Session` 返回零值，节点查询仍正常。
- `Claim` 原子更新两个字段；`Release` 原子比较节点和会话标识，只删除匹配的在线节点，返回是否实际释放。节点为空且标识非空表示已离线，重复释放返回 false。
- 离线标识不会自动过期，避免迟到事件被误认为当前事件。记录占用为每个用户、每个使用过的网关服务一个标识；数据清理须与事件保留策略一起规划。
- `Bind/UnBind` 用于业务节点定位。`UnBind` 原子比较节点后删除。相同服务一旦使用会话接口，普通绑定和解绑会返回错误，避免绕过会话归属校验。
- `service`、写操作的 `node` 和会话标识必须非空；所有操作传递调用方的上下文和 Redis 错误。
- 此版本直接采用新字段结构，不读取历史 `service -> node` 字段；部署前须清理旧位置数据并重建在线归属。

**验证**

`go test -mod=readonly ./...` 和 `go test -mod=readonly -race ./...` 使用 miniredis 执行 Lua，覆盖同节点和跨节点重连、并发占用与释放、离线记录、路由投影、普通解绑的并发替换以及失败路径。双实例模拟主从槽位和不同会话数据，验证读取主节点而非滞后副本；这些测试不替代真实 Redis Cluster 联调。

