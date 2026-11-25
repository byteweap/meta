# 外层消息协议与 Gate 转发

用户于 2026-11-17 确认优化 Gate 上行转发、要求 `header` 必填，并明确下行状态和业务消息体的约定。变更涉及公共协议及 Gate、Mesh 两个组件，采用 Level 2 追溯。

| 验收项 | 要求 |
|---|---|
| AC-01 | Gate 解码并检查上行消息，按目标服务路由，将收到的 Protobuf 字节原样发布。保留字段编号、类型、Broker 接口签名和路由方式。 |
| AC-02 | 上下行协议要求 `header` 存在。Gate 拒绝缺失头部的上行包，Mesh 在业务路由处复核。框架下行响应始终构造头部。 |
| AC-03 | `RESPONSE` 省略 `result` 表示成功，失败携带非零 int32 错误码，`PUSH` 省略 `result`。`ErrResp` 对零或越界错误码记录错误并回复通用失败码 500。 |
| AC-04 | 定义请求关联、路由版本、时间戳来源、上下行服务名与 payload 选择规则，文档与现有业务解码行为一致。 |

`header` 的必填性通过检查消息字段是否存在来实现，头部标量仍按原有零值和路由约定处理。`seq` 只用于同一连接内的请求关联，重连可重用已结束连接的序号。推送和无需关联的消息使用 0。

下行 Gate 保持现有字节透传。框架自带的响应构造满足协议，业务自行构造推送时负责填充合法头部与消息类型。服务间 Request/Reply 和会话事件沿用各自的协议。

实施和验证见 [进度记录](progress.md)。

## 2026-11-18 消息类型命名

用户指出 `IMessage` 和 `OMessage` 无法直观体现方向。本次选用 `ClientEnvelope` 表示客户端上行，`ServerEnvelope` 表示服务端响应和推送。IDL、Go 调用方及当前文档统一采用新名称，字段和枚举保持原有线包格式。

用户确认无需兼容旧类型名。Go 调用方、Protobuf 反射查找和 `Any` 类型 URL 统一使用新名称。

## 2026-11-18 字段命名

| 验收项 | 要求 |
|---|---|
| AC-05 | `Header.sent_at_unix_ms` 表示发送方的 Unix 毫秒时间，保留字段 #4、`int64`、零值表示未提供的约定以及 Mesh 给响应填入当前时间的行为。 |
| AC-06 | 下行使用 `kind` 和 `error`，错误类型为 `ResponseError`，内部字段为 `code` 和 `message`。保留原字段编号、类型结构、枚举值和成功省略错误的规则。 |
| AC-07 | 保留 `seq`、`cmd`、`version` 和 `ClientEnvelope.header`。本次仅分析 `seq` 在业务有序性上的用途，不引入单调校验、排队、重放或重连处理变更。 |

普通 Protobuf 二进制帧保持兼容；生成的字段和类型名、反射名以及默认 ProtoJSON 键随 IDL 改名。已有调用方需要按新名称迁移。

## 2026-11-25 Protobuf 命名规范

用户要求通过修改协议定义解决 Buf lint 警告。沿用外层协议的 Level 2 追溯。

| 验收项 | 要求 |
|---|---|
| AC-08 | 外层协议使用 `envelope.v1` 包名和与包名匹配的目录。枚举值使用 `MSG_TYPE_` 前缀，零值使用 `_UNSPECIFIED` 后缀。通过 Buf `STANDARD` 检查。 |
| AC-09 | 保留字段编号、字段类型、枚举数值和 Go 导入路径。同步生成命令、Go 绑定、框架调用方、示例及文档。 |
| AC-10 | `make envelope` 和 `buf generate` 使用同一模板，每次生成选用 Buf Schema Registry 上最新的 Go 插件。外层协议的 Go 绑定输出至 `envelope/envelope.pb.go`。 |

旧客户端和服务端可继续交换普通 Protobuf 二进制帧。重新编译的调用方使用新的 Go 枚举常量。使用 Protobuf 全限定名、`Any` 类型 URL 或 ProtoJSON 枚举字符串的调用方需要同步迁移。
