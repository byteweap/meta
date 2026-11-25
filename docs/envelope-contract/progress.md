# 外层消息协议实施与验证

2026-11-17 已确认 [变更范围与验收标准](requirements.md)。实施涉及 Gate 上行转发、Mesh 业务路由入口和响应构造，同步维护 IDL 与 README。

| 验收项 | 实现与验证 |
|---|---|
| AC-01 | [Gate 入口](../../server/gate/gate_handler.go) 解码校验后把本次 WebSocket 读取的原始切片交给 [分发](../../server/gate/gate_message.go)。读取端为每条消息提供独立缓冲区，[Broker](../../component/broker/broker.go) 实现将数据视为只读。网关回归测试以不同字段顺序构造报文，断言发布字节与输入一致。新旧 Protobuf 描述符逐字节一致。 |
| AC-02 | Gate 和 [Mesh 业务入口](../../server/mesh/mesh.go) 都拒绝缺少头部的上行消息。[Gate 回归](../../server/gate/gate_envelope_test.go) 同时覆盖损坏的头部。[Mesh 回归](../../server/mesh/route_fast_test.go) 证实缺失头部不会命中零值路由，显式空头部仍可使用原路由。 |
| AC-03 | [响应回归](../../server/mesh/context_error_test.go) 通过捕获真实发布数据验证成功省略 `result`，合法错误码和提示照常传递，零和越界错误码回传 500。框架响应均包含 `header`。 |
| AC-04 | [IDL](../../idl/envelope/v1/envelope.proto) 和 [README](../../README.md) 明确上下行字段语义。已重新生成 [Go 绑定](../../envelope/envelope.pb.go)。 |

根模块的 `go test -mod=readonly ./...` 全部通过，包含 Gate 的本机 WebSocket 测试。`make check` 构建了 21 个模块。构建期间有沙箱禁止写入 Go 模块元数据缓存的提示，返回状态为成功。

已复核代码差异与验证记录。验证使用本机回环连接及 Broker 替身。实际 NATS 部署环境和业务自行构造的推送由其集成验证覆盖。

2026-11-18 复核未提交改动。临时目录重新生成的 Go 绑定与工作区文件逐字节一致，当前和基线的协议描述符仍一致。补齐 [Mesh 路由说明](../../README.md#业务路由) 与 [Wrap 注释](../../server/mesh/handler.go) 中空 payload 的约定。Gate、Mesh 定向回归和 21 个模块构建均通过。仓库内的 Gate 客户端示例尚未实现协议收发。

## 2026-11-18 消息类型命名

从已合入上阶段改动的 `dev` 创建 `refactor/envelope-names`。已同步 [IDL](../../idl/envelope/v1/envelope.proto)、Go 绑定、Gate、Mesh、[README](../../README.md) 和架构图。按用户要求移除旧 Go 类型别名，项目只使用新类型名。

命名变更通过了根模块测试。将旧 IDL 的两处类型名替换为新名后，生成的描述符与当前描述符逐字节一致。

移除别名后，确认 Go 和 Proto 源码已无旧类型名。Mesh 测试、Gate 定向回归和 21 个模块的 `make check` 通过。

## 2026-11-18 字段命名与验证

| 验收项 | 实施与证据 |
|---|---|
| AC-05 | [IDL](../../idl/envelope/v1/envelope.proto) 将发送时间命名为 `sent_at_unix_ms`。[响应回归](../../server/mesh/context_error_test.go) 验证 `Context.Timestamp()` 仍读取客户端时间，成功与失败响应均使用 Mesh 当前时间。 |
| AC-06 | 下行改用 `kind`、`error` 和 `ResponseError.code/message`。[响应回归](../../server/mesh/context_error_test.go) 验证成功省略错误，错误码和说明照常传递，零和越界错误码仍回传 500。 |
| AC-07 | `seq`、`cmd`、`version`、上行结构和实际路由行为不变。[README](../../README.md) 明确框架不以 `seq` 排序或去重。严格业务顺序由状态归属方处理，不从请求关联号或发送时间推断；重连续传和跨连接迟到回包隔离不在本次范围内。 |

使用 `protoc-gen-go v1.36.11` 执行 `make envelope`。在临时副本中还原当前 IDL 的五处改名后，生成的 Protobuf 描述符与基线逐字节一致。Gate/Mesh 定向测试、根模块 `go test -mod=readonly ./...`、`make check`（21 个模块）和 `examples/game` 模块测试均通过。未验证外部客户端或生产 Broker；依赖生成 API、反射、`Any` 类型 URL 或 ProtoJSON 键的调用方需迁移。

## 2026-11-25 Protobuf 命名规范

| 验收项 | 实现与验证 |
|---|---|
| AC-08 | [IDL](../../idl/envelope/v1/envelope.proto) 使用 `envelope.v1` 和符合规范的枚举名。[Buf 配置](../../buf.yaml) 以 `idl` 为模块根。根目录的 `buf lint` 和目标文件的 `buf format --diff --exit-code` 均通过。 |
| AC-09 | 用 `make envelope` 和 `protoc-gen-go v1.36.11` 更新 [Go 绑定](../../envelope/envelope.pb.go)，独立重新生成的文件与工作区逐字节一致。同步了调用方、示例和 [迁移说明](../../README.md#消息与集群协议)。将文件路径、包名和枚举名还原后，新旧 Protobuf 描述符一致。请求、成功响应、失败响应和推送四种二进制帧编码一致，并能用新旧 IDL 双向解码。 |

根模块及 `examples/game`、`examples/gate-client` 的 `go test -mod=readonly ./...` 检查通过，`make check` 构建 21 个模块。另行执行的 `make lint` 报告 11 条 Go 告警，已在改动前的 `dev` 基线上逐条复现。验证范围为仓库内测试与本地协议样例。

生成模板最初使用本机 `protoc-gen-go v1.36.12` 验证，输出为 `envelope/envelope.pb.go`。使用同一插件对比 Buf 和 `protoc` 的生成结果，差异仅在文件头的编译器版本注释。

AC-10 的 [生成模板](../../buf.gen.yaml) 使用 Buf Schema Registry 上的最新 Go 插件。`make envelope` 调用同一模板，并限定外层协议文件。[开发命令](../../README.md#开发命令) 已说明 Buf CLI 和网络要求。已通过 `buf lint`、目标文件的 `buf build` 和 `make -n envelope`。远端生成及其输出校验等待协议描述外发授权。

## 2026-11-25 统一 Protobuf 工具链的初次迁移

| 验收项 | 实施与证据 |
| --- | --- |
| AC-11 | [Buf workspace](../../buf.yaml) 包含两个模块。`buf ls-files` 列出全部 3 个协议文件。根目录的 `buf lint`、`buf build` 以及两个模块的 `buf format --diff --exit-code` 均通过。 |
| AC-12 | [示例 Makefile](../../examples/game/Makefile) 改用 Buf，移除了两份 Docker 脚本。在临时副本中把现有模板的远端插件替换为本地 `protoc-gen-go v1.36.12`，分别运行 `make envelope`、`make -C examples/game proto` 和 `buf generate`，按预期输出 1、2、3 个文件。全部 Go 绑定与仓库版本的差异仅在工具版本注释。 |
| AC-13 | 与迁移前的 `HEAD` 比较，两个模块的 Protobuf 描述符集合逐字节一致，`buf breaking` 通过。在临时副本中新增使用 `package game` 的文件时，Buf 仍报告包目录和版本规则违规，验证例外只覆盖已有文件。 |

根模块及 `examples/game` 的 `go test -mod=readonly ./...`、`make check`（21 个模块）通过。[开发命令](../../README.md#开发命令) 已同步。实现和本地验证完成。沿用 AC-10 的 BSR 插件配置，远端生成涉及向 `buf.build` 发送协议描述，本次未执行远端调用。

## 2026-11-25 按 STANDARD 规则调整游戏协议

用户要求移除初次迁移中的 lint 例外。更新后的 AC-13 由 [事件协议](../../examples/game/proto/game/v1/event.proto) 和 [RPC 协议](../../examples/game/proto/game/v1/rpc.proto) 使用 `game.v1` 包名及 `game/v1/` 路径实现。[Buf 配置](../../buf.yaml) 只启用 `STANDARD`，两个模块的 `buf lint`、`buf build` 和格式检查均通过。

对比迁移前后的描述符，移除源位置信息并还原两处包名和文件路径后，描述符完全一致。默认 `FILE` 模式的 `buf breaking` 报告两条预期的 `FILE_NO_DELETE`，对应旧的 `event.proto` 和 `rpc.proto`。依赖 Protobuf 全限定名、`Any` 类型 URL 和旧描述符文件路径的调用方需要迁移，普通 Protobuf 二进制消息保持兼容。[开发命令和迁移说明](../../README.md#开发命令)

使用 Buf 和本地 `protoc-gen-go v1.36.12` 通过 `make -C examples/game proto` 重生成两份 Go 绑定。默认 BSR 生成未实跑。根模块和游戏示例测试、`make check` 以及 `git diff --check` 均通过。

复核了仓库内的 Proto 导入、游戏事件与 RPC 处理器，以及直接引用 Protobuf 全限定名、`Any` 类型 URL 或 Go 描述符变量的代码。业务调用均使用原有 Go 消息类型，仓库内没有需要改写的旧引用。已补全 [迁移说明](../../README.md#开发命令) 中 Proto 导入路径和 Go 描述符变量的对应关系。
