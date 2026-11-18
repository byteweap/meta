# 外层消息协议实施与验证

2026-11-17 已确认 [变更范围与验收标准](requirements.md)。实施涉及 Gate 上行转发、Mesh 业务路由入口和响应构造，同步维护 IDL 与 README。

| 验收项 | 实现与验证 |
|---|---|
| AC-01 | [Gate 入口](../../server/gate/gate_handler.go) 解码校验后把本次 WebSocket 读取的原始切片交给 [分发](../../server/gate/gate_message.go)。读取端为每条消息提供独立缓冲区，[Broker](../../component/broker/broker.go) 实现将数据视为只读。网关回归测试以不同字段顺序构造报文，断言发布字节与输入一致。新旧 Protobuf 描述符逐字节一致。 |
| AC-02 | Gate 和 [Mesh 业务入口](../../server/mesh/mesh.go) 都拒绝缺少头部的上行消息。[Gate 回归](../../server/gate/gate_envelope_test.go) 同时覆盖损坏的头部。[Mesh 回归](../../server/mesh/route_fast_test.go) 证实缺失头部不会命中零值路由，显式空头部仍可使用原路由。 |
| AC-03 | [响应回归](../../server/mesh/context_error_test.go) 通过捕获真实发布数据验证成功省略 `result`，合法错误码和提示照常传递，零和越界错误码回传 500。框架响应均包含 `header`。 |
| AC-04 | [IDL](../../idl/envelope/envelope.proto) 和 [README](../../README.md) 明确上下行字段语义。已重新生成 [Go 绑定](../../envelope/envelope.pb.go)。 |

根模块的 `go test -mod=readonly ./...` 全部通过，包含 Gate 的本机 WebSocket 测试。`make check` 构建了 21 个模块。构建期间有沙箱禁止写入 Go 模块元数据缓存的提示，返回状态为成功。

已复核代码差异与验证记录。验证使用本机回环连接及 Broker 替身。实际 NATS 部署环境和业务自行构造的推送由其集成验证覆盖。

2026-11-18 复核未提交改动。临时目录重新生成的 Go 绑定与工作区文件逐字节一致，当前和基线的协议描述符仍一致。补齐 [Mesh 路由说明](../../README.md#业务路由) 与 [Wrap 注释](../../server/mesh/handler.go) 中空 payload 的约定。Gate、Mesh 定向回归和 21 个模块构建均通过。仓库内的 Gate 客户端示例尚未实现协议收发。
