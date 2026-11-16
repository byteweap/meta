# 第 4 步实施与验证进度

## 当前状态

- 需求见 [应用就绪与失败清理](requirements.md)，关键流程见 [决策记录](decision.md)。
- 第 4 步已提交为 `9f11c11`；16 步复审发现与 Polaris 回滚冲突。补充修复、本地验证与独立复审已完成，用户已确认提交。
- 追溯级别为 Level 2，并记录生命周期接口选择。
- 基线提交为 `0d0a8a1`。
- 最后更新为 2026-11-16。

## 复审修复：注册失败清理归属

- 发现：App 在注册失败后注销整个实例，会删除 Polaris 已明确保留的预存端点。
- 方案：App 仅清理成功注册；失败回滚由适配器负责。同步补齐 Nacos/Eureka 部分成功回滚及 etcd 已确认租约清理，保留全部错误。公共方法签名不变。
- 实现：[App](../../app.go) 以注册成功标记控制注销；[接口约定](../../component/registry/registry.go) 与 README 同步。Nacos 先解析全部端点，失败回滚成功前缀；Eureka 逐项独立限时回滚并停止心跳，已成功响应不因随后取消改报失败；etcd 写入失败撤销本次 lease ID 并在启动注册失败时关闭 lease 客户端。
- 验收证据见下表。原发现通过真实 App + Polaris 适配器复现：旧实现删除记录为 `[9001 9000 9001 9002]`；修复后只删除新建 `9001`，预存 `9000` 保留。
- 审查状态：独立代理已只读复审完整修复，覆盖 App、Nacos/Eureka/etcd 回滚、取消、错误保留及新增测试，未发现新增可行动问题；原 Polaris 误注销发现已修复。以下 2026-11-11 结果属于原第 4 步历史证据。

| 验收项 / 检查 | 本次证据 |
|---|---|
| AC-08 | [Polaris App 回归](../../contrib/registry/polaris/app_lifecycle_test.go) 6 个场景验证失败/取消与回滚错误、正常退出及 PostStart 失败；[App 测试](../../app_test.go) 验证成功响应紧邻取消仍注销，失败不注销而服务仍停止 |
| AC-09 | [Nacos 回归](../../contrib/registry/nacos/registration_rollback_test.go) 验证预解析、成功前缀回滚、全部错误保留及取消边界；[Eureka 回归](../../contrib/registry/eureka/registration_rollback_test.go) 验证部分失败、取消、清理错误/超时后继续、心跳回收与成功响应；[etcd 回归](../../contrib/registry/etcd/registration_rollback_test.go) 验证 Grant 失败无副作用、Put 失败仅撤销已知租约、独立清理 context、错误聚合和成功响应 |
| 根 App race | `go test -mod=readonly -race -count=1 .` 通过（1.561s） |
| Polaris 普通 / race | 模块内 `go test -mod=readonly ./...` 与 `go test -mod=readonly -race ./...` 通过（0.596s / 1.673s） |
| Eureka 普通 / race | 模块内相同命令通过（0.765s / 1.702s），修复前新增两组回归失败 |
| Nacos 普通 / race | 模块内相同命令通过（0.495s / 1.913s） |
| etcd 专项 race | `go test -mod=readonly -race -run '^TestRegisterWithKVRollsBackOnlyGrantedLease$' -count=1 ./...` 通过（1.850s） |
| 21 模块构建 | `make check` 通过，退出 0；模块 stat 缓存仍有权限提示，不影响构建，依赖文件未改动 |
| 文档与差异核对 | 12 个变更 Go 文件格式检查、41 个本地文档链接及 `git diff --check` 均通过 |

本次命令使用 `GOCACHE=/tmp/meta-review-module-cache GOPROXY=off`。测试均使用组件替身和可控 HTTP transport，不连接真实注册中心。etcd 原有全量测试需要运行中的 etcd，本次仅运行新增回归；未运行真实服务联调或线上验证。SDK 超时后的远端结果仍可能不确定，回滚失败保留错误供调用方处理；Nacos/Eureka 无 Existed 标记，无法区分同地址预存实例，本次保证仅回滚本次成功返回的端点。范围扩展仅为同步失败清理职责所必需的三种适配器调整，未更改公共方法签名、依赖或增加兼容分支。

2026-11-16 用户确认提交本次修复。本记录随实现与回归测试一并提交，提交说明为 `fix: scope service deregistration to successful registrations`。

## 实施记录

| 日期 | 工作 | 状态 |
|---|---|---|
| 2026-11-11 | 用户确认第 4 步方案 | 已确认 |
| 2026-11-11 | 明确就绪、单次运行和统一清理契约 | 已记录 |
| 2026-11-11 | App 统一启动、就绪等待和失败清理 | 已完成，Stop 发出请求，Run 汇总清理结果 |
| 2026-11-11 | Gate/Mesh 就绪与启动停止同步 | 已完成，提前监听器纳入清理，正常 HTTP 停止不误报 |
| 2026-11-11 | NATS 订阅确认 | 已完成，确认有界、失败退订，沿用调用方上下文控制存续期 |
| 2026-11-11 | App、Gate/Mesh、NATS 回归测试及针对性 race | 已通过 |
| 2026-11-11 | 21 个模块构建 | 全部通过 |
| 2026-11-11 | 独立代码审查及最终测试断言复核 | 已完成，无未解决发现 |
| 2026-11-11 | 用户批准第 4 步提交 | 已确认 |

## 验证证据

| 检查 | 状态 | 证据或待办 |
|---|---|---|
| AC-01 | 已通过 | [App 测试](../../app_test.go) 验证全部就绪后才注册，启动失败、意外返回和就绪前停止均结束等待 |
| AC-02 | 已通过 | App 注入 Endpoint、PreStart、Start、Register 和 PostStart 失败，断言清理；[Gate 测试](../../server/gate/gate_lifecycle_test.go) 验证 Endpoint 提前监听后可停止并重新绑定同一地址 |
| AC-03 | 已通过 | App 测试逐项用 `errors.Is` 检查启动、多个停止钩子、注销和服务停止错误，断言全部钩子仍执行；清理超时仍保留已返回的启动错误 |
| AC-04 | 已通过 | App 测试验证清理上下文保留值和 AppInfo、未继承运行取消且有 deadline；16 个并发停止请求只清理一次，钩子内停止无死锁，超时后仍执行 PostStop |
| AC-05 | 已通过 | Gate 和 [Mesh 测试](../../server/mesh/mesh_lifecycle_test.go) 验证订阅前不就绪、订阅成功后就绪、订阅失败不就绪、提前停止以及订阅期间停止；正常 Gate 停止返回 nil |
| AC-06 | 已通过 | [NATS 测试](../../contrib/broker/nats/subscription_ready_test.go) 直接核对服务端目标主题/队列订阅并从独立连接发布首条消息；预取消、确认超时、调用方更短 deadline 和确认期间取消均检查退订 |
| AC-07 | 已通过 | 下列模块测试、针对性 race、21 模块构建均退出 0；独立审查 App、配置、Server 接口、Gate/Mesh、相关测试及最后 NATS 断言，无未解决发现 |
| 文档与差异 | 已通过 | 追溯文档内容和本地链接检查通过；修改的 Go 文件均已 gofmt，`git diff --check` 退出 0 |

本地验证使用 `GOCACHE=/tmp/meta-review-module-cache GOPROXY=off`，执行结果如下。

| 目录 | 命令 | 结果 |
|---|---|---|
| 仓库根目录 | `go test -mod=readonly . ./server/gate ./server/mesh` | 通过 |
| 仓库根目录 | `go test -mod=readonly -race .` | 通过，包括启动错误与清理超时同时发生的回归 |
| 仓库根目录 | `go test -mod=readonly -race ./server/gate ./server/mesh -run 'Test(Gate\|Mesh)(Lifecycle\|Auth)'` | 通过 |
| `contrib/broker/nats` | `go test -mod=readonly ./...` | 通过 |
| `contrib/broker/nats` | `go test -mod=readonly -race ./... -run 'TestSubscribe\|TestContextErrFastFail\|TestQueueSubscribe'` | 通过 |
| 仓库根目录 | `make check` | 21 个模块全部构建成功 |

初版 NATS 就绪测试的全局订阅计数混入了服务内部订阅；改为使用服务端 `Subsz` 只检查 `t.ready` 主题及队列，随后完整 NATS 测试和相关 race 检查通过。该调整修正验证范围，生产订阅实现没有因此变更。

## 偏差、风险与剩余工作

当前没有实施范围偏差。真实注册中心、线上部署和实际操作系统退出信号未做集成验证；本地测试使用组件替身、临时连接和内嵌 NATS。race 检查覆盖本次生命周期、认证和订阅路径，未扩展到全仓所有包。不遵守 context 的业务函数无法被强制终止。第 5 步 panic 恢复已确认，进度见后续链接；第 6 步网关完整资源管理待确认。

全模块构建有模块 stat 缓存目录不可写提示，但全部构建成功，依赖声明与校验文件未变化。

## 完成范围与下一步

实现涉及 [App](../../app.go)、[配置](../../option.go)、[Server 接口](../../server/server.go)、[Gate](../../server/gate/gate.go)、[Mesh](../../server/mesh/mesh.go) 和 [NATS](../../contrib/broker/nats/broker.go)。同步更新 [Broker 契约](../../component/broker/broker.go)、[README](../../README.md)、[NATS 使用说明](../../contrib/broker/nats/README.md)、测试与第 3 步进度链接。NATS 使用说明中的既有失效示例名称已一并对齐当前 API。

本步骤采用必需 Ready 接口和单次生命周期，没有增加历史兼容分支或依赖。默认清理预算为 10 秒，Stop 发出请求后立即返回，Run 返回表示已完成清理或已达到等待期限，并携带完整的已收集错误。

需求、决策、实施、验证证据和审查状态已同步。第 4 步已提交为 `9f11c11`，提交说明为 `fix: wait for server readiness and centralize app cleanup`。后续从 [第 5 步进度](../message-panic-recovery/progress.md) 继续，第 6 至 16 步待确认。
