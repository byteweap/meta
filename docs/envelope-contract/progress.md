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

## 2026-11-18 消息类型命名

从已合入上阶段改动的 `dev` 创建 `refactor/envelope-names`。已同步 [IDL](../../idl/envelope/envelope.proto)、Go 绑定、Gate、Mesh、[README](../../README.md) 和架构图。按用户要求移除旧 Go 类型别名，项目只使用新类型名。

命名变更通过了根模块测试。将旧 IDL 的两处类型名替换为新名后，生成的描述符与当前描述符逐字节一致。

移除别名后，确认 Go 和 Proto 源码已无旧类型名。Mesh 测试、Gate 定向回归和 21 个模块的 `make check` 通过。

## 2026-11-18 字段命名与验证

| 验收项 | 实施与证据 |
|---|---|
| AC-05 | [IDL](../../idl/envelope/envelope.proto) 将发送时间命名为 `sent_at_unix_ms`。[响应回归](../../server/mesh/context_error_test.go) 验证 `Context.Timestamp()` 仍读取客户端时间，成功与失败响应均使用 Mesh 当前时间。 |
| AC-06 | 下行改用 `kind`、`error` 和 `ResponseError.code/message`。[响应回归](../../server/mesh/context_error_test.go) 验证成功省略错误，错误码和说明照常传递，零和越界错误码仍回传 500。 |
| AC-07 | `seq`、`cmd`、`version`、上行结构和实际路由行为不变。[README](../../README.md) 明确框架不以 `seq` 排序或去重。严格业务顺序由状态归属方处理，不从请求关联号或发送时间推断；重连续传和跨连接迟到回包隔离不在本次范围内。 |

使用 `protoc-gen-go v1.36.11` 执行 `make envelope`。在临时副本中还原当前 IDL 的五处改名后，生成的 Protobuf 描述符与基线逐字节一致。Gate/Mesh 定向测试、根模块 `go test -mod=readonly ./...`、`make check`（21 个模块）和 `examples/game` 模块测试均通过。未验证外部客户端或生产 Broker；依赖生成 API、反射、`Any` 类型 URL 或 ProtoJSON 键的调用方需迁移。
