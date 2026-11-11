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

- `Claim(ctx, uid, service, node, sessionID)` 原子绑定新会话，返回是否替换在线会话
- `Release(ctx, uid, service, node, sessionID)` 仅释放匹配的当前在线会话，返回是否实际释放
- `Session(ctx, uid, service)` 原子读取最后的 `Session{ID, Node}`；Node 为空表示离线

每次连接必须使用新的非空 ID，调用顺序以原子 Claim 的执行顺序为准。释放后保留最后 ID，供 Mesh 过滤旧状态事件；不自动过期或保留完整会话历史。同一服务不能混用 Bind/UnBind 与 Claim/Release。

## 最小用法

以下示例使用 `contrib/locator/redis`：

```go
package main

import (
  "context"
  "crypto/rand"

  rloc "github.com/byteweap/meta/contrib/locator/redis"
  "github.com/redis/go-redis/v9"
)

func main() {
  opts := redis.UniversalOptions{Addrs: []string{"127.0.0.1:6379"}}
  loc := rloc.New(opts, "wk")
  defer loc.Close()

  ctx := context.Background()
  sessionID := rand.Text()
  _, _ = loc.Claim(ctx, 1001, "gate", "node-1", sessionID)
  _, _ = loc.Node(ctx, 1001, "gate")
  _, _ = loc.Release(ctx, 1001, "gate", "node-1", sessionID)

  // 普通业务节点继续使用 Bind/UnBind。
  _ = loc.Bind(ctx, 1001, "game", "game-1")
  _ = loc.UnBind(ctx, 1001, "game", "game-1")
}
```

## 实现（contrib）

- Redis: `github.com/byteweap/meta/contrib/locator/redis`
