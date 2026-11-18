# Mesh 客户端请求处理实施记录

在 `feat/mesh-typed-client-handlers` 分支的 `0475156` 基线上，已完成[本批需求](requirements.md)及本地验证。调用方式与迁移说明见 [README](../../README.md#业务路由)。

## 实现取舍

- [Handle](../../server/mesh/handle.go) 在注册时包装业务函数，复用现有 Wrap 的解码和上下文管理。自动回包只尝试一次，业务 panic 在调用处转为错误，发布 panic 由外层消息循环处理。
- [客户端注册与分发](../../server/mesh/mesh.go) 统一使用 `Route`，注册时原子检查命令与版本冲突。已移除 `RouteX` 及其反射适配，调用方按 [README](../../README.md#业务路由) 迁移。旧代码若通过重复注册覆盖 handler，迁移时应保留最终需要的那次注册。
- [回包编码](../../server/mesh/context.go) 共用原有路由信息和 envelope。请求上下文移除了未读取的事件字段，UID 编解码使用具体的十进制整数操作。
- [game 示例](../../examples/game/internal/app.go) 的进入和退出游戏请求均使用类型化 handler，处理完成后回复空的成功响应。后续的[服务间 RPC 截止时间](../mesh-rpc-deadline/progress.md)单独记录。客户端业务 handler 的 context 仍跟随 Mesh 生命周期。

## 验证

| 检查 | 结果 |
|---|---|
| [退出游戏确认](../../examples/game/internal/handler/event/game_exit_test.go) | 对示例的 ExitGame 处理器捕获一次成功回包，响应体为空且 seq、cmd、version 与请求一致。game 模块测试通过 |
| [路由与回包测试](../../server/mesh/handle_test.go) | 覆盖 Handle 和 Wrap 的解码失败、重复注册后保留原 handler、未知命令、公开与内部错误、panic、响应编码失败和空包。核对回复地址、UID、会话 ID 与请求关联字段 |
| 根模块与 game 模块 | 各自运行 `go test -mod=readonly ./...` 通过 |
| 并发与构建 | `go test -mod=readonly -race ./server/mesh` 和 `make check` 通过 |
| 静态检查 | `golangci-lint run ./server/mesh ./internal/cluster` 无问题，`git diff --check` 通过 |

## 性能基准

[BenchmarkClientDispatch](../../server/mesh/handler_bench_test.go) 使用预编码的 Gate 帧，业务消息为包含 128 字节数据的 Protobuf BytesValue，handler 原样返回该消息。每次测量从 Mesh 消息入口开始，覆盖路由查找、解码、业务调用、回包编码、构造 Broker 头及调用空操作 Pub。输入构造在计时区间之外，网络与 Broker 服务端耗时未纳入。

环境为 Apple M1、Go 1.27.1、darwin/arm64，使用 `-cpu=1 -benchmem -count=5`，耗时取五次中位数。

| 路径 | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 改动前手动回包 | 1208 | 1532 | 22 |
| 改动后手动回包 | 1099 | 1516 | 21 |
| 改动后类型化返回值 | 1105 | 1516 | 21 |
