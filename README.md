# Meta

<p align="center">
  <img src="assets/logo.svg" alt="Meta logo" width="560">
</p>

<p align="center">
  <a href="https://go.dev/"><img src="https://img.shields.io/github/go-mod/go-version/byteweap/meta" alt="Go version"></a>
  <a href="https://codecov.io/gh/byteweap/meta"><img src="https://codecov.io/gh/byteweap/meta/branch/master/graph/badge.svg" alt="Codecov"></a>
  <a href="https://goreportcard.com/report/github.com/byteweap/meta"><img src="https://goreportcard.com/badge/github.com/byteweap/meta" alt="Go Report Card"></a>
</p>

Meta 是面向长连接游戏和实时业务的 Go 服务框架。Gate 接入 WebSocket 客户端，Mesh 执行业务路由和服务间 RPC；注册发现、消息代理、玩家定位、节点选择和日志通过组件接口接入，业务服务可以按服务名部署多个实例。

当前核心能力包括：

- **客户端请求与推送**：Protobuf 外层协议、类型化请求处理器、按 UID 主动推送，以及支持并发请求的 Go 客户端
- **动态路由**：按需发现业务服务，维护实例快照；支持玩家绑定路由和服务级无状态路由
- **会话归属**：连接认领、条件释放、Gate 运行租约，以及绑定原连接的响应投递
- **服务间 RPC**：定点调用、按服务名选点、截止时间传递，以及可选的固定 worker 并发
- **运行控制**：就绪后注册、停止排空、客户端队列过载拒绝和请求阶段观测

分房、匹配、房间迁移、状态同步和业务限流由业务服务负责。

[架构与目录](#架构与目录) · [运行示例](#运行示例) · [消息协议](#消息协议) · [Gate](#gate) · [Mesh](#mesh) · [生命周期](#生命周期) · [组件与扩展](#组件与扩展) · [开发与验证](#开发与验证) · [升级注意事项](#升级注意事项) · [实现边界](#实现边界)

## 架构与目录

![Meta 架构总览](assets/architecture.svg)

| 层次 | 职责 |
| --- | --- |
| `meta.App` | 管理服务启动、就绪、实例注册、钩子和停止清理 |
| `server/gate` | 验证握手身份，维护连接与会话，发现并转发业务消息，投递响应和推送 |
| `server/mesh` | 执行客户端路由、会话事件和服务间 RPC，提供主动推送与排空 |
| `component` | 定义 Broker、Registry、Locator、Selector、Observer 和 Logger 契约 |
| `contrib` | 对接 NATS、Redis、注册中心和日志设施；按独立 Go module 引入 |
| 业务服务 | 组合 `*mesh.Mesh`，维护玩家、房间等领域状态及其归属 |

一个 App 可以管理多个 `server.Server`，它们共享 App 的 ID、服务名和一次实例注册，不会各自注册为不同业务服务。Gate 和业务 Mesh 通常分别运行在独立进程中；Mesh 的 `mesh://` endpoint 是注册信息，实际服务间通信走 Broker。

```text
.
├── app.go / option.go          # 应用生命周期和配置
├── client/gate/                # WebSocket Go 客户端
├── server/
│   ├── server.go               # Server、Drainer 契约
│   ├── gate/                   # 网关、会话、转发和连接清理
│   └── mesh/                   # 业务路由、RPC、推送和排空
├── component/                  # broker、registry、locator、selector、observe、log
├── contrib/                    # 可选设施适配器，独立模块
├── idl/envelope/v1/            # 外层 Protobuf 协议源文件
├── envelope/                   # 生成的 Go 绑定
├── encoding/                   # proto、json、msgpack、yaml、toml、xml 编解码器
├── internal/                   # 集群主题与消息头、服务发现缓存、观测保护
├── pkg/                        # 并发 map、转换、网络地址和异步等工具
├── examples/                   # gate、game、gate-client，分别为独立模块
├── cmd/meta/                   # CLI 命令骨架，new/run 尚未实现
└── docs/                       # 各项变更的需求、设计决策和验证记录
```

## 运行示例

### 环境准备

当前根模块、`contrib` 和示例的 `go.mod` 声明 **Go 1.27.1**，独立的 `cmd/meta` 模块声明 Go 1.25.5，开发时以所在模块的声明为准。从仓库检出代码后，示例通过各自 `go.mod` 中的本地 `replace` 使用当前实现，无需先发布或安装框架 CLI。

运行现有 Gate 和 game 示例，需要准备以下本机服务：

| 依赖 | 示例连接地址 | 用途 |
| --- | --- | --- |
| Nacos | `127.0.0.1:18848`，ContextPath `/nacos` | 注册发现，默认 group/cluster |
| Redis | `127.0.0.1:6379` | 玩家位置和 Gate 会话，键前缀 `meta` |
| NATS | `nats://localhost:4222` | NATS Core 消息通信 |

示例中的设施地址写在源码中；使用其他地址或认证方式时，需要修改对应初始化配置。这里的 Nacos 端口是 **18848**。完整初始化见 [Gate 入口](examples/gate/cmd/main.go)、[game 入口](examples/game/cmd/main.go)和 [game 组件组装](examples/game/internal/app.go)。

### 启动服务与客户端

以下三个终端都从仓库根目录开始。先在终端一启动 game：

```bash
cd examples/game
go run ./cmd
```

在终端二配置单账户演示令牌并启动 Gate：

```bash
export GATE_DEMO_TOKEN='replace-with-your-demo-token'
cd examples/gate
go run ./cmd
```

Gate 示例在 **8000–8999** 中随机选择监听端口，以日志中的 `[websocket] server listening on:` 为准。在终端三设置同一令牌，并将下方端口替换为日志中的实际端口：

```bash
export GATE_DEMO_TOKEN='replace-with-your-demo-token'
cd examples/gate-client
go run ./cmd -url ws://127.0.0.1:实际端口/
```

客户端通过 `Authorization: Bearer <token>` 完成握手，再向 `game` 的 `(cmd=1, version=1)` 发送进入游戏请求，并打印 `ServerEnvelope`。当前 EnterGame/ExitGame 处理器只记录日志并返回空的成功响应；示例中的玩家、房间和座位结构是业务扩展起点，尚未串成完整游戏流程。

演示令牌对应 UID 1，未配置令牌时 Gate 示例直接退出。实际业务应在 `gate.UserIDExtractor` 中验证自己的凭据并返回正数 UID，不能直接信任请求中的 `uid`。示例服务 ID 也采用小范围随机数，部署时应改成唯一实例 ID。

### 接入自己的服务

下面是组装片段：`bro` 实现 `broker.Broker`，`loc` 实现 `locator.SessionLocator`，`reg` 实现 `registry.Registry`，`authenticate` 为业务身份验证函数。它们的创建和错误处理可参考上面的完整示例。

```go
g := gate.New(
    gate.Addr(":9000"),
    gate.UserIDExtractor(authenticate),
    gate.Broker(bro),
    gate.Locator(loc),
    gate.Discovery(reg),
    gate.SelectorFunc(func() selector.Selector { return wrr.New() }),
)

err := meta.New(
    meta.ID("gate-1"),
    meta.Name("gate"),
    meta.Server(g),
    meta.Registry(reg),
).Run()
```

业务服务使用另一个 App，注册自己的服务名，并在启动前注册路由：

```go
m := mesh.New(mesh.Broker(bro), mesh.Locator(loc))
m.Route(1, 1, mesh.Handle(EnterGame))
m.Route(2, 1, mesh.Handle(ExitGame))
m.RPCRoute("hello", "v1", mesh.WrapRPC(Hello))

err := meta.New(
    meta.ID("game-1"),
    meta.Name("game"),
    meta.Server(m),
    meta.Registry(reg),
).Run()
```

Gate/Mesh 的 Broker subject 前缀默认都是 `meta`，通信双方需要一致；它与 Redis 定位器构造时的键前缀分别配置。Gate 和 Mesh 必须访问同一份会话存储。App 不负责关闭注入的 Broker、Locator 或 SDK 客户端，资源应在 `Run()` 返回后或 `PostStop` 中关闭。

## 消息协议

[外层协议源文件](idl/envelope/v1/envelope.proto)是客户端消息契约，Protobuf 包名为 `envelope.v1`，Go 导入路径为 `github.com/byteweap/meta/envelope`。Proto 导入以 `idl/` 为根：

```proto
import "envelope/v1/envelope.proto";
```

| 消息 | 方向 | 主要字段 |
| --- | --- | --- |
| `ClientEnvelope` | 客户端 → Gate → Mesh | 必填 `header`、非空目标 `service`、业务 `payload` |
| `ServerEnvelope` | Gate/Mesh → 客户端 | 必填 `header`、来源 `service`、`kind`、可选 `error` 和 `payload` |
| `Header` | 上下行共用 | `seq`、`cmd`、`version`、`sent_at_unix_ms` |

客户端路由由 `(service, cmd, version)` 决定。业务 payload 是该路由约定的 Protobuf 字节，空 payload 表示没有业务消息体。`kind` 只能使用 `MSG_TYPE_RESPONSE` 或 `MSG_TYPE_PUSH`；成功响应和推送省略 `error`，失败响应带非零 `error.code` 和可选说明。

`seq` 关联请求与响应，响应回填原值，0 表示无需关联，推送使用 0。框架不按它排序或去重。`sent_at_unix_ms` 是发送方的 Unix 毫秒时间戳，0 表示未提供，客户端时钟不用于判断消息先后。

集群元数据放在 Broker 消息头中，包括 `uid`、`event`、`reply`、`from_service`、`to_service`、`session_id` 和 `delivery`。主题格式为：

```text
<prefix>.<fromService>.<toService>.<toNodeID>
meta.gate.game.game-1
```

Gate/Mesh 订阅 `<prefix>.*.<appName>.<appID>`。客户端转发和下行响应走 pub-sub，回复地址放在消息头 `reply`；服务间 RPC 走 Broker request-reply，使用 `broker.Message.Reply`。这两种回复地址承担不同职责。

`meta.Version` 是注册实例版本，客户端的 `header.version` 和 RPC 的字符串 `version` 是业务路由版本，三者分别配置。

## Gate

Gate 基于 Melody 管理 WebSocket，只解析上行外层协议和转发元数据，业务 payload 保持原样，下行帧也不重新解码。

### 身份与会话

Gate 启动需要 `UserIDExtractor`、`SessionLocator`、`Broker`、`Discovery` 和 `SelectorFunc`。身份验证函数在每次握手前调用一次，返回非正数时回复 HTTP 401，成功后 UID 直接进入会话。

连接建立时，Gate 为该连接生成新的会话 ID，用 `SessionLocator.Claim` 记录 Gate 节点、会话 ID 和本次 Gate 运行 ID。认领成功后才替换本地旧连接；明确失败时关闭新连接并保留旧连接。同一网关按 UID 串行切换连接，不阻塞其他用户。

断开时，`Release` 只在节点和会话 ID 都匹配时释放归属，成功释放才发送 `offline`。`online/reconnect/offline` 广播给用户已绑定的业务节点；Mesh 查询当前会话身份和在线状态后再执行事件钩子，跳过过期事件。

每次 Gate 运行共用一份 **60 秒租约，每 15 秒续期**。租约失效后，`Session` 查询和 UID 推送将该次运行的会话视为离线；存储拒绝续期或最后确认的有效期耗尽时，Gate 启动停止流程。租约过期本身不会生成 `offline`，正常释放才是该事件的来源。

### 服务发现与转发

新增业务服务注册可用实例后即可接收客户端请求，无需重启 Gate 或维护公开服务白名单。Gate 根据 `service` 查询实例，`cmd/version` 是否存在由 Mesh 判断。

1. 首次请求查询注册中心；没有可用实例时回复 404。同服务的并发请求共用初始化
2. 建立监听后再查询一次快照，后续请求使用本地快照；实例清空或监听失败时缓存失效，下次请求重新发现
3. 全部可用实例均声明 `stateless=true` 时直接选点；否则先查询用户在目标业务服务的绑定。已有绑定只转发到快照中的原实例，绑定实例失效时回复 503；无绑定时调用选择器
4. 向目标 subject 发布原始客户端帧，携带 UID、原连接会话 ID 和回复地址

无状态声明通过 App 元数据配置，作用于整个服务：

```go
meta.Metadata(map[string]string{registry.MetadataStateless: "true"})
```

Gate 选点时把 UID 作为 key，但内置 Random、RoundRobin、WRR 都忽略 key。**选点不会自动建立业务绑定**：有状态业务应自行调用 `Locator.Bind/UnBind` 管理玩家归属。Gate 会话使用 `Claim/Release`，同一服务名不能混用这两组操作。注册发现快照与后续通知没有统一的版本协议，不提供强一致视图。

### 响应投递与错误

Mesh 响应标记 `delivery=response` 并回填原请求的 `session_id`。Gate 仅向身份匹配的本地连接投递；推送标记 `delivery=push`，不带会话 ID，按 UID 投递给当前本地连接。

`gate.StrictSessionDelivery` 默认关闭，兼容缺少标记或会话 ID 的旧发布者；这时旧回包仍可能误投给新连接。所有回包方迁移后应启用严格模式，详见[升级注意事项](#升级注意事项)。

| 场景 | 行为 |
| --- | --- |
| 外层 Protobuf 无法解析或缺少 header | 记录日志并丢弃，客户端等待超时 |
| 服务不存在或当前没有可用实例 | 在原连接回复 404 |
| 发现、定位、选点或发布失败，或绑定实例已失效 | 在原连接回复 503 |
| 发布成功后业务未回包 | 客户端按自己的截止时间结束等待 |

Gate 生成的错误响应也回填原 `seq/cmd/version`，包括 `seq=0`，来源服务为 Gate。详细组件错误只记录在服务端。

### 默认配置

| 选项 | 默认值 |
| --- | --- |
| `Addr` / `Path` | `:9000` / `/` |
| `MaxMessageSize` | 2048 字节 |
| `WriteTimeout` / `PongTimeout` / `PingInterval` | 5 秒 / 60 秒 / 10 秒 |
| `WebsocketMessageBufferSize` / `BrokerMessageBufferSize` | 各 256 |
| `StrictSessionDelivery` / `Observer` | 关闭 / nil |

`MessageBufferSize` 可同时设置 WebSocket 和 Broker 缓冲区。完整选项见 [gate/option.go](server/gate/option.go)。

## Mesh

Mesh 启动必须配置 Broker 和 SessionLocator；`Discovery` 和 `SelectorFunc` 仅在使用 `RequestService` 时需要。默认使用容量为 **256** 的串行消息队列，客户端消息、会话事件和入站 RPC 在同一消息循环中执行。

### 客户端路由

有请求、有响应的业务使用 `mesh.Handle`：

```go
func EnterGame(ctx context.Context, caller mesh.Caller, req *pb.EnterGameRequest) (*pb.EnterGameResponse, error) {
    return &pb.EnterGameResponse{}, nil
}

m.Route(1, 1, mesh.Handle(EnterGame))
```

这里的 `pb` 为业务生成的 Protobuf 包。`caller.UID` 来自 Gate 的认证结果，可按值保留。空 payload 传入 nil 请求，`nil, nil` 返回空的成功响应。handler 的标准 context 随 Mesh 执行上下文取消；客户端取消等待不会撤销已经发出的业务操作。

业务错误可返回 `&mesh.ClientError{Code: 409, Message: "room full"}`，支持 `%w` 包装。Code 必须非零，Message 必须是有效 UTF-8。普通错误、无效业务错误、编码失败或类型化处理器 panic 返回通用 500；业务 payload 解码失败返回 400，路由不存在返回 404。错误响应依赖可解析的外层 header 和可用回复地址。

需要单向处理或自行控制回包时，使用 `mesh.Wrap`：

```go
m.Route(2, 1, mesh.Wrap(func(ctx *mesh.Context, req *pb.ExitGameRequest) {
    ctx.OkResp()
}))
```

`*mesh.Context` 提供 UID、序号、命令、版本、来源、时间戳及 `OkResp/ErrResp`。对象在 handler 返回后归还池，异步保留必须先 `Copy()`；复制不会延长 Mesh 的运行期限，也不自动保证业务状态并发安全。同一客户端 `cmd/version` 重复注册会 panic。

### 会话事件与主动推送

```go
m.OnlineHandler(func(uid int64) {})
m.OfflineHandler(func(uid int64) {})
m.ReconnectHandler(func(uid int64) {})

// Mesh 就绪后，notice 为业务 Protobuf 消息，nil 表示空消息体
err := m.Push(ctx, uid, 20, 1, notice)
```

事件通过 Broker 的 `event` 头区分，不占用业务路由。推送通过 `Session` 定位当前在线 Gate，默认逻辑服务名为 `gate`；自定义 Gate 名称时，用 `mesh.GateService("edge")` 对应 Gate 的 `meta.Name("edge")`。

用户离线可用 `errors.Is(err, mesh.ErrUserOffline)` 判断。定位、编码或发布错误返回调用方；返回 nil 仅表示 Broker 接受发布。NATS 此时可能仍在发送缓冲区，不能据此确认客户端已收到。定位后用户迁移 Gate 时，本次推送可能丢失或被旧连接收到。

### 服务间 RPC

RPC 使用独立的字符串命令和版本，handler 返回原始字节、提示和状态码：

```go
func Hello(ctx *mesh.RPCContext, req *HelloReq) ([]byte, string, int) {
    return []byte("hello"), "", http.StatusOK
}

m.RPCRoute("hello", "v1", mesh.WrapRPC(Hello))

ctx, cancel := context.WithTimeout(parent, 200*time.Millisecond)
defer cancel()
data, tip, code, err := m.Request(ctx, "meta.game.room.room-1", "hello", "v1", payload)
```

`HelloReq` 为业务 Protobuf 类型。`WrapRPC` 解码请求；`RPCContext.Context()` 可传给数据库或下一次 RPC。`RPCRouteX` 仍提供反射签名适配，高频路径使用 `RPCRoute + WrapRPC`。

`err` 表示调用未取得 RPC 响应，取得响应后还需检查 `code`，200 才表示成功。业务 panic 返回通用 500；非 200 回包不携带业务数据。配置 Broker 后，定点 `Request` 在 Mesh 启动前也可调用。

按服务名选点时，为 Mesh 配置发现和选择器：

```go
m := mesh.New(
    mesh.Broker(bro),
    mesh.Locator(loc),
    mesh.Discovery(reg),
    mesh.SelectorFunc(func() selector.Selector { return roundrobin.New() }),
)

// Mesh 就绪后调用，ctx 覆盖发现、传输、排队和业务执行
data, tip, code, err := m.RequestService(ctx, "room", "create", "v1", payload)
```

`RequestService` 维护按服务名的发现缓存，以空 key 选点，只发送一次，失败不自动重试。无可用实例可用 `errors.Is(err, selector.ErrNoAvailableNode)` 判断。适合任意实例都能处理的操作；加入已有房间等操作由业务确定归属后定点调用。

每次 RPC 都应设置截止时间。框架将绝对 Unix 毫秒期限写入 `deadline_unix_ms`，节点时钟需要同步。排队时间计入预算，出队已过期时跳过业务并回复 504；执行中到期通知 handler，返回后丢弃结果并按 504 处理。期限头格式错误回复 400。没有期限时沿用 Mesh 上下文；主动取消只结束本地等待，不向远端发送取消消息。RPC 回包使用独立的发送期限，并受停止预算约束。

服务名和实例 ID 应使用非空、不含点号、空白或 Broker 通配符的单段名称。`RequestService` 和推送路径会校验主题段，Mesh 启动也会拒绝前缀、服务名或实例 ID 中的 `*`、`>`，避免订阅扩大到其他实例。

### 并发、过载与故障隔离

| 选项 | 默认行为 | 启用后的行为 |
| --- | --- | --- |
| `MessageBufferSize(n)` | 串行队列容量 256 | 设置客户端消息、会话事件和默认串行 RPC 的队列容量 |
| `ClientOverloadRejection(true)` | 客户端消息等待入队 | 队列满时快速拒绝；每个 Mesh 最多一个在途 503 尝试，预算最多 100 毫秒，其余只计数 |
| `RPCConcurrency(workers, queueSize)` | `(0, 0)`，RPC 串行执行 | RPC 单独交给固定 worker；执行及等待名额耗尽时回复 503 |

例如 `mesh.RPCConcurrency(2, 16)` 最多执行 2 个 RPC，另有 16 个等待名额；`queueSize=0` 仍可使用 worker 执行名额。负参数、容量溢出或只配置队列会使启动失败。

启用 RPC 并发后，RPC 彼此之间、RPC 与客户端处理器和会话事件之间都可能并行，业务需要同步共享玩家或房间状态。客户端过载回包名额已占用或请求缺少有效回包信息时，不保证返回 503，客户端仍须设置超时。同步嵌套 RPC 应避免循环等待，串行消息循环和有限 worker 都可能因此阻塞。

Gate/Mesh 在单条 Broker 消息处理入口恢复 panic 并记录堆栈。类型化客户端处理器和 RPC 业务处理器有对应的 500 回包路径，其他消息处理或传输 panic 不保证回包。业务自行启动的 goroutine 需自行设置恢复边界，恢复不撤销外部副作用。

## 生命周期

`meta.New(...).Run()` 执行一次完整生命周期：

1. 根据 App 身份和各 Server 的 endpoint 构建一个注册实例，执行 `PreStart`
2. 并发启动所有 Server，等待全部 `Ready` 通道关闭后注册，再执行 `PostStart`
3. 等待退出信号、父 context 取消、`App.Stop()` 或服务退出；启动失败也进入清理流程
4. 对支持 `server.Drainer` 的服务调用 `BeginDrain`，执行 `PreStop`，注销已成功注册的实例
5. 停止所有配置的 Server，等待已启动的 `Start` 返回，执行所有 `PostStop`，合并返回错误

App 和内置 Server 实例都只能运行一次。`App.Stop()` 只发出停止请求，可重复调用或在钩子中调用；需要等待 `Run()` 返回才能确认清理完成。`RegistryTimeout` 默认为 10 秒，全部清理共享 `StopTimeout`，默认也为 10 秒。任一步清理失败仍继续执行后续清理。

自定义 Server 实现 [server.Server](server/server.go)：`Kind/Start/Ready/Stop/Endpoint`。`Ready` 从构造起返回同一通道，必要初始化和订阅生效后关闭；`Start` 应持续运行到停止或失败；`Stop` 应能清理 Endpoint 提前分配的资源，也能在尚未启动时调用。支持排空时可额外实现 `server.Drainer`。

- **Mesh**：停止接纳新消息，在预算内完成已入队工作，RPC 继续遵守并发上限与请求期限；预算耗尽时取消执行和回包。已接纳 handler 在排空期间仍可访问依赖，故依赖应在 `PostStop` 或 `Run()` 返回后关闭
- **Gate**：停止新连接，关闭现有连接，等待断开回调、消息循环、订阅和发现任务退出；解绑使用独立清理 context，每次最多 10 秒，停止后的回调还受首次 Stop context 约束

重复 `Gate.Stop(ctx)` 和 `Mesh.Stop(ctx)` 等待同一次清理，各次调用的 context 限制自己的等待；超时不代表后台工作已终止。Mesh 首次独立 Stop 未给期限时使用 10 秒预算；直接取消独立 `Mesh.Start` 的 context 会立即取消执行。无法响应 context 的 handler 或 SDK 调用可能超出停止预算。

## 组件与扩展

| 契约 | 作用 | 仓库内实现 |
| --- | --- | --- |
| [broker.Broker](component/broker/broker.go) | Pub/Sub/Request/Reply/Close | [NATS Core](contrib/broker/nats) |
| [registry.Registry](component/registry/registry.go) | 注册、注销、查询、监听 | [Nacos](contrib/registry/nacos)、[Consul](contrib/registry/consul)、[Etcd](contrib/registry/etcd)、[Eureka](contrib/registry/eureka)、[Polaris](contrib/registry/polaris)、[ServiceComb](contrib/registry/servicecomb)、[ZooKeeper](contrib/registry/zookeeper) |
| [locator.SessionLocator](component/locator/locator.go) | 业务绑定、Gate 运行租约和会话归属 | [Redis](contrib/locator/redis) |
| [selector.Selector](component/selector/selector.go) | 按快照选点，可附加过滤器 | [加权随机](contrib/selector/random)、[轮询](contrib/selector/roundrobin)、[平滑加权轮询](contrib/selector/wrr) |
| [observe.Observer](component/observe/observe.go) | 阶段耗时和框架拒绝事件 | 业务实现并通过 Gate/Mesh 的 `Observer` 注入 |
| [log.Logger](component/log/log.go) | 结构化日志、Helper、Valuer 和 Filter | 标准日志、[Zap](contrib/log/zap)、[Logrus](contrib/log/logrus)、[Zerolog](contrib/log/zerolog)、[Aliyun](contrib/log/aliyun)、[Tencent](contrib/log/tencent)、[Fluent](contrib/log/fluent) |
| [encoding.Codec](encoding/encoding.go) | 编解码器注册与查找 | proto、json、msgpack、yaml、toml、xml |

业务只需引入使用的 `contrib` 模块。新增设施适配器放在 `contrib/<component>/<provider>`，核心不直接依赖 provider SDK。编码器在导入各子包时注册；现有 Gate/Mesh 消息链路固定使用 Protobuf，注册其他 Codec 不会自动切换传输格式。

内置选择器构造函数均为 `New()`，支持并发访问。加权选择器读取实例 metadata 的 `weight`，非正权重按 1 处理；这与注册中心是否将实例认定为可路由是两层规则。自定义注册中心应保留 App 实例 ID、原服务名和业务元数据，特别是 `stateless`；Nacos 使用 metadata 中的 `id` 还原 App ID，不能用 Nacos 自身 InstanceId 代替。

Broker 成功订阅应表示服务端订阅已生效。NATS 适配器为此执行 Flush；Pub/Reply 成功表示接受发布，Subscription.Close 和 Broker.Close 调用异步 Drain，返回不表示所有回调或缓冲消息都已处理完成。NATS Core 适配器不提供持久化、离线补发或端到端确认。注册中心的 SDK 超时、心跳和监听释放能力也各不相同，App 的清理预算不能强制终止无 context 的 SDK 调用。

业务服务可像 [game Server](examples/game/internal/server/server.go) 一样组合 `*mesh.Mesh` 并持有领域状态。工具包中的 [cmap](pkg/cmap/README.md) 使用 `New(hash, shardCount)` 显式构造，分片配置固定在实例上；跨分片遍历不保证原子快照。

### 请求观测

`gate.Observer(obs)` / `mesh.Observer(obs)` 默认不启用。回调在请求路径同步执行，可能并发，应快速返回并支持并发；框架保护并记录其 panic。

| 阶段 | 测量范围 |
| --- | --- |
| `Routing` | Gate 查找服务、定位、选择目标，止于 Broker 发布前 |
| `Queue` | Mesh 尝试入队到出队，包含默认满队列等待时间 |
| `Handler` | 执行业务处理器；客户端处理器同步回包也计入 |
| `Reply` | Mesh 向 Broker 提交客户端响应或 RPC 回包，止于适配器返回 |

框架拒绝原因包括 `Unavailable`、`QueueFull`、`DeadlineExceeded` 和 `Stopping`；业务自行返回的错误码不会触发拒绝计数。会话事件、主动推送和出站 RPC 不参与这些观测，阶段耗时也不等于客户端端到端延迟。

### Go 客户端

[client/gate](client/gate/client.go) 的 `Dial(ctx, url, header, options...)` 建立单条 WebSocket 连接，ctx 仅控制握手；连接后的生命周期由 `Close` 和网络状态控制。

- `Request(ctx, service, cmd, version, payload)` 支持并发，自动分配 seq 并匹配响应。Go error 表示未取得响应，业务失败须检查 `response.GetError()`
- `Pushes()` 独立接收推送，缓冲为 32 条；消费不及时会丢弃新推送，不阻塞响应分发
- 默认同进程跨连接继续递增 seq。只有所有 Gate 开启严格投递且发布者完成迁移后，才可用 `WithConnectionSequence()` 让每条连接从 1 开始
- 断线后由调用方重新 Dial 并同步业务状态，不自动重连或重放请求。取消等待不保证已发送请求没有执行

## 开发与验证

从仓库根目录运行：

```bash
make check                  # 逐个构建所有 Go 模块，-mod=readonly
make test                   # 等价于 go test -v ./...，仅根模块
golangci-lint run ./...      # 根模块静态检查，需先安装兼容的 golangci-lint v2
make lint                   # 同上；缺失时尝试自动安装，命令见下方说明
make tidy                   # 先嵌套模块、后根模块执行 go mod tidy
buf lint                    # 两个协议模块，STANDARD 规则
buf build                   # 编译两个协议模块
buf generate                # 生成外层协议和游戏示例绑定
make envelope               # 只生成外层协议绑定
make -C examples/game proto # 只生成游戏示例绑定
git diff --check            # 检查补丁空白问题
```

仓库没有 Go workspace，根目录的 `go test ./...` 和 lint 不会覆盖嵌套模块。修改适配器、CLI 或示例时，需要在受影响模块中分别运行测试和 lint；跨模块变更另运行 `make check`。Go 代码修改需 gofmt，并通过 `go test -cover` 核对每个受影响包的语句覆盖率严格大于 95%，完整验收要求见 [AGENTS.md](AGENTS.md)。

`.golangci.yml` 使用 v2 配置；当前 Makefile 的缺失安装分支仍调用不带 `/v2` 的旧模块路径，因此应先准备兼容版本的 golangci-lint。`make check` 只构建，不代替模块测试或实际设施联调。

Buf 的 [workspace](buf.yaml) 包含 `idl/` 和 `examples/game/proto/`，[生成模板](buf.gen.yaml) 共用远端 Go 插件。生成绑定需要 Buf CLI 和 `buf.build` 网络访问，协议描述会发送给远端插件；lint/build 可在本地运行。变更外层协议后运行 `make envelope` 并检查生成差异。

依赖整理也可使用 `./tidy.sh` 或 Windows 的 `./tidy.ps1`。`release.sh <version>` / `release.ps1 <version>` 会改写依赖版本、提交、推送并为模块打标签；当前 Makefile 没有可用的 `make release` 目标。发布流程为 `dev` 合入 `master` 后从 `master` 打标签，任务分支的 PR 以 `dev` 为目标。

## 升级注意事项

- **客户端处理器**：旧 `RouteX` 已移除。`func(*mesh.Context, *T)` 改用 `Route(cmd, version, mesh.Wrap(fn))`；有标准请求/返回值的业务使用 `mesh.Handle`。直接持有 `MessageHandler` 时传给 `Route`。服务间 `RPCRouteX` 仍保留
- **会话存储**：Gate/Mesh 现在要求 `SessionLocator`。Redis 字段和状态事件格式已更新，不兼容旧数据格式，升级需协调定位器、Gate、Mesh 及自定义事件发布者，见[会话归属](docs/session-ownership/decision.md)和[运行租约](docs/gate-session-expiry/decision.md)
- **响应隔离**：先升级 Gate/Mesh，再迁移自定义发布者：响应必须设置 `delivery=response` 并复制原 `session_id`，推送使用 `delivery=push` 且不带会话 ID；Broker 适配器必须保留这些头。完成后启用 `gate.StrictSessionDelivery(true)`，见[迁移与回退](docs/session-reply-isolation/decision.md)
- **Protobuf 命名**：外层包为 `envelope.v1`，Proto 导入路径为 `envelope/v1/envelope.proto`；游戏示例包为 `game.v1`，导入路径为 `game/v1/event.proto` 和 `game/v1/rpc.proto`。Go 绑定目录未变，普通二进制帧兼容，但枚举常量、默认 ProtoJSON 枚举字符串、描述符变量、全限定名和 `Any` 类型 URL 的引用需要同步，见[外层协议记录](docs/envelope-contract/progress.md)
- **并发 map**：全局 `ShardCount` 和单参数构造已移除，使用 `cmap.New(hash, shardCount)` 并处理返回错误，见 [cmap 文档](pkg/cmap/README.md)

更多设计和验证依据见[应用生命周期](docs/application-lifecycle/decision.md)、[动态发现](docs/gate-discovery/decision.md)、[无状态路由](docs/gate-stateless-routing/decision.md)、[RPC 并发](docs/mesh-rpc-concurrency/requirements.md)、[停服排空](docs/mesh-request-drain/decision.md)和[观测契约](docs/request-observability/requirements.md)。这些记录的历史验证结果以各自记录的范围为准。

## 实现边界

- Gate 当前业务入口只处理二进制 Protobuf 帧，文本消息回调仍为空
- Gate 收到 Broker request-reply 请求回复 501；客户端请求通过 pub-sub 转发到 Mesh
- `mesh.Context.Broadcast` 尚未实现，按 UID 推送使用 `Mesh.Push`
- 会话归属不等于跨网关强制踢线。旧网关连接可能仍存活，响应隔离也不会撤销旧请求已产生的业务操作
- 会话事件不提供可靠投递，状态校验与业务回调不构成跨系统事务。远端写入成功但响应丢失时，调用失败不代表位置没有改变
- 框架不提供房间归属迁移、请求重试/去重、业务状态恢复或完整游戏逻辑；这些需要业务自行设计
- `cmd/meta new` 和 `cmd/meta run` 仅有命令定义，目前不会生成项目或启动服务

本项目采用 [Apache License 2.0](LICENSE)。
