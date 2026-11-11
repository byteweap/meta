# 第 13 步实施与验证进度

## 当前状态

- [需求](requirements.md) 已确认，[设计决策](decision.md) 已记录。
- 实现、本地验证和独立审查已完成，用户已确认提交；基线为第 12 步提交 `8445092`。
- Level 2 + 决策记录：缓存、订阅和实例生命周期并发语义变化。
- 最后更新：2026-11-11。

## 实施记录

| 日期 | 工作 | 状态 |
|---|---|---|
| 2026-11-11 | 用户确认第 13 步四项方案 | 已确认 |
| 2026-11-11 | 核查缓存/通知及客户端共享状态根因 | 范围已明确 |
| 2026-11-11 | 单一刷新、独立 watcher、可取消等待及元数据隔离 | 实现完成 |
| 2026-11-11 | 五组发现回归及真实测试标签适配 | 普通/race 与 integration 编译通过，查询等待取消断言已补齐 |
| 2026-11-11 | 请求局部服务器顺序及按实例管理注册/心跳/注销 | 实现及五组客户端回归完成 |
| 2026-11-11 | 独立审查发现预取消操作不得影响现有心跳、停机后清理上下文仍需可注销 | 两项已修复并通过专项回归 |
| 2026-11-11 | 最终完整测试、race、集成编译及 21 模块构建 | 全部通过 |
| 2026-11-11 | 独立审查与追溯核对 | 已完成 |
| 2026-11-11 | 用户确认提交第 13 步 | 已确认，本记录随实现一并提交 |

## 验证与审查

发现回归见 [discovery_concurrency_test.go](../../contrib/registry/eureka/discovery_concurrency_test.go)，客户端回归见 [client_concurrency_test.go](../../contrib/registry/eureka/client_concurrency_test.go)，真实服务测试见 [registry_test.go](../../contrib/registry/eureka/registry_test.go)。

| 验收项 / 检查 | 结果与证据 |
|---|---|
| AC-01 | TestEurekaDiscoverySnapshotsAndIsolation、EmptyAndFailureRecovery、RefreshSerializationAndCancellation 验证首空/首错、部分删除→空→恢复、失败保留旧缓存、先更新再通知及合并刷新请求不并行 |
| AC-02 | TestEurekaDiscoveryIndependentWatchers 验证同服务三个监听独立、八轮通知不阻塞慢消费者、停止一个不影响其余；ConcurrentReadersAndSubscriptions 覆盖 32 个并发读/订阅任务 |
| AC-03 | 发现回归验证单个查询等待取消不影响共享刷新、Registry 取消终止在途 HTTP、父 watcher context 取消、并发重复 Stop 以及取消创建返回 nil；客户端回归覆盖预取消操作不误停现有心跳 |
| AC-04 | 发现回归修改返回快照后缓存不变，多个 endpoint 的注册元数据互不污染且输入 map 不变；客户端重注册请求仍使用登记时复制的元数据 |
| AC-05 | TestEurekaClientConcurrentRequestsAndRetryBody 验证 16 个并发请求、重试服务器顺序、完整 body、地址输入隔离及最后底层错误；FetchErrorsAndCancellation 验证 HTTP 错误和在途取消 |
| AC-06 | IndependentHeartbeats 验证同服务两实例心跳、重复注销和任务记录回收；DeregisterOrdersReregistration 验证取消/等待旧重注册后 DELETE、新注册不会越过旧任务；DeregisterAfterRegistryCancellation 验证运行取消后独立清理 context 仍可注销 |
| AC-07 | 默认完整测试、race、集成编译和全部模块构建通过；独立审查完成，记录已同步 |
| 默认完整测试 | Eureka 模块 `go test -mod=readonly ./...`，十个顶层测试组通过，退出 0（0.891s） |
| race | Eureka 模块 `go test -mod=readonly -race ./...`，全部默认测试通过，退出 0（2.009s） |
| integration 编译 | Eureka 模块 `go test -mod=readonly -tags integration -run '^$' ./...`，退出 0（0.665s），未运行真实测试 |
| 模块构建 | 根目录 `make check` 覆盖全部 21 个模块，退出 0 |
| 独立审查 | 已完成；发现并修复预取消操作副作用及停机后注销两个问题，复核实现与回归后无剩余可行动问题 |
| 格式与文档 | 七个变更 Go 文件通过 gofmt 核查，差异检查及追溯内容/本地链接检查通过 |

验证统一设置 `GOCACHE=/tmp/meta-review-module-cache GOPROXY=off`。测试使用真实 http.Client 配可控 Transport 和 testing/synctest 虚拟时间，不创建监听端口，不新增依赖或生产测试钩子。构建出现全局模块 stat 缓存不可写提示，但全部构建成功。

真实 Eureka 联调未执行，未验证真实服务端部署、鉴权或远端处理请求的事务顺序；原示例测试改为 integration 标签下的有界断言，删除不验证项目行为的 TestLock。独立审查为只读复核，不把测试或文档当作真实部署验收。

## 边界与下一步

无实质范围偏差。保持原有 endpoint 解析及默认刷新间隔，不新增依赖或历史兼容；失败时保留成功缓存，没有额外过期策略。远端已收到请求的效果无法由本地 context 保证撤销。注册/注销失败不会自动恢复已停止的旧心跳，由调用方决定重试；Registry context 取消后仍允许独立清理 context 注销。

需求、决策、实现、验收证据与审查状态已同步，第 13 步已提交为 `582e726`。用户已确认 [第 14 步 Polaris 注册方案](../polaris-registration/requirements.md)，后续实施与验证见该主题记录。
