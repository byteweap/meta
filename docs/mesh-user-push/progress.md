# 按 UID 主动推送实施记录

[验收范围](requirements.md)对应基线 `010fd43`。[调用示例](../../README.md#按-uid-主动推送)给出配置、返回值和重连时的投递语义。

## 实现与自查

- `Mesh.Push` 每次原子读取一次 `Session(uid, gateService)`。在线节点为空时返回 `mesh.ErrUserOffline`，避免为离线用户编码。Gate 自定义服务名可通过 `mesh.GateService` 设置，默认值为 `gate`。[发送实现](../../server/mesh/push.go)
- 复用现有 Gate 单播主题和外层协议。推送使用 `delivery=push`，省略 `session_id`，`ServerEnvelope` 的序号为零。按本次定位结果发布一次，跨 Gate 重连及异常退出后的旧位置均有投递不确定性。[下行契约](../session-reply-isolation/decision.md)
- 自查已完成。复现了定位耗尽预算后取消定时器尚未调度、过期推送仍进入 Broker 的情况。现在在定位前、定位后和即将发布时同步检查绝对期限。已开始的发布返回 Broker 的结果。[定向回归](../../server/mesh/push_test.go)
- Redis 定位器的默认套接字读取曾忽略调用方期限。适配器现在启用 go-redis 的上下文超时，作用于所有定位操作。回归直接检查 `HMGET` 的 TCP 读取期限。[Redis 回归](../../contrib/locator/redis/session_test.go)
- game 和 Gate 示例现在共用 Redis 会话键前缀 `meta`。使用其他前缀的部署需要让两个服务使用相同的值。[game 示例](../../examples/game/internal/app.go)

## 验证

单 Mesh 通过受控定位器和 Broker 核对发布次数、目标节点、消息头以及解码后的推送帧。既有 Gate WebSocket 和客户端用例分别验证严格模式接受不带会话标识的推送，以及客户端通过 `Pushes()` 分发消息。[Gate 回归](../../server/gate/gate_session_ownership_test.go) · [客户端回归](../../client/gate/client_test.go)

根模块按约定跳过两项单进程多节点用例，其余根模块测试、game 示例测试和 21 个模块的构建通过。Mesh 和 Redis 定位器分别通过三轮竞态检查。Mesh lint 及 Redis、game 示例的增量 lint 通过。Redis 全量 lint 报告两处基线已有的 `gofumpt` 格式问题，位于 `locator_test.go` 和 `session_test.go`。

额外使用一个 Mesh 向实际 NATS 服务发布推送，由独立订阅端核对主题、UID、投递头和解码后的消息。用 go-redis 经 TCP 连接 miniredis，阻塞 `HMGET` 响应后验证定位读取在调用方期限耗尽时返回超时。这两项专项检查均通过三轮竞态运行。

## 本地开销

Apple M1、Go 1.27.1，`-cpu=1 -benchmem -count=3`。在线推送复用 Mesh 上下文与携带 128 字节业务数据的 Protobuf BytesValue，内存定位器直接返回节点，Broker 立即返回。计时覆盖定位调用、两层 Protobuf 编码、构造主题与头、发布调用。修复后的三次中位数为 798.3 ns/op，1024 B/op，15 allocs/op。直接 RPC 和缓存命中服务 RPC 均为 480 B/op、8 allocs/op。[推送基准](../../server/mesh/push_test.go) · [RPC 基准](../../server/mesh/request_service_test.go)
