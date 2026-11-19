# Locator

## 说明

Locator 用于跟踪玩家在各服务节点上的位置，提供绑定、解绑与查询能力。

## 接口概览

核心接口位于 `component/locator`：

- `ID()` 实现标识
- `AllNodes` 查询用户在所有服务上的节点
- `Node` 查询用户在某服务上的节点
- `Bind` 绑定用户到某服务节点
- `UnBind` 解绑用户与某服务节点
- `Close` 关闭定位器

Gate 和 Mesh 使用 `SessionLocator`，它包含上述接口并增加：

- `AcquireGateLease(ctx, runID, ttl)` 为一次 Gate 运行建立租约，运行 ID 须全新且不可复用
- `RenewGateLease(ctx, runID, ttl)` 延长已有租约，租约已失效时返回 false
- `Claim(ctx, uid, service, node, sessionID, runID)` 写入新会话与有效的 Gate 运行 ID，返回是否替换尚未释放的位置
- `Release(ctx, uid, service, node, sessionID)` 仅释放节点和会话 ID 匹配的归属，返回是否实际释放
- `Session(ctx, uid, service)` 读取最后的会话并校验 Gate 租约。返回的 Node 为空时表示离线

Gate 每次启动持有一份运行租约，租约至少为 1 毫秒。每次连接使用新的会话 ID，并发认领以原子写入顺序为准。释放或租约过期后保留最后会话 ID，供 Mesh 过滤状态事件。判断 Gate 会话在线状态时用 `Session`，`Node` 和 `AllNodes` 返回记录中的位置。同一服务不能混用 Bind/UnBind 与会话接口。

## 最小用法

以下示例使用 `contrib/locator/redis`：

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
  opts := redis.UniversalOptions{Addrs: []string{"127.0.0.1:6379"}}
  loc := rloc.New(opts, "wk")
  defer loc.Close()

  ctx := context.Background()
  runID := rand.Text()
  _ = loc.AcquireGateLease(ctx, runID, time.Minute)
  sessionID := rand.Text()
  _, _ = loc.Claim(ctx, 1001, "gate", "node-1", sessionID, runID)
  _, _ = loc.Session(ctx, 1001, "gate")
  _, _ = loc.Release(ctx, 1001, "gate", "node-1", sessionID)

  // 普通业务节点继续使用 Bind/UnBind。
  _ = loc.Bind(ctx, 1001, "game", "game-1")
  _ = loc.UnBind(ctx, 1001, "game", "game-1")
}
```

## 实现（contrib）

- Redis: `github.com/byteweap/meta/contrib/locator/redis`
