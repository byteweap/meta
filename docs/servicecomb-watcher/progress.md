# 第 12 步实施与验证进度

## 当前状态

- [需求](requirements.md) 已确认，[设计决策](decision.md) 已记录。
- 实现、本地验证及独立审查已完成，用户已确认提交；基线为第 11 步提交 `9cb7be2`。
- Level 2 + 决策记录：监听和并发模型变化。
- 最后更新：2026-11-11。

## 实施记录

| 日期 | 工作 | 状态 |
|---|---|---|
| 2026-11-11 | 用户确认第 12 步及轮询/取消边界 | 已确认 |
| 2026-11-11 | 核查 SDK callback、重连、revision 与查询接口 | 方案已确定 |
| 2026-11-11 | Next 驱动完整查询、两级可取消串行门、共用转换及旧事件接口移除 | 实现完成 |
| 2026-11-11 | 五组可控客户端回归，使用虚拟时间验证间隔和取消 | 普通及 race 回归通过 |
| 2026-11-11 | 真实 SDK 配本地 HTTP 回归及旧测试适配 | 已完成，普通及 race 通过 |
| 2026-11-11 | 取消创建监听时返回真正 nil 接口，避免失败清理误调用 Stop | 已修复并补充断言 |
| 2026-11-11 | ServiceComb 完整测试、race 及全部 21 模块构建 | 最终修改后全部通过 |
| 2026-11-11 | 独立审查及追溯核对 | 已完成 |
| 2026-11-11 | 用户确认提交第 12 步 | 已确认，本记录随实现一并提交 |

## 验证与审查

可控回归见 [watcher_snapshot_test.go](../../contrib/registry/servicecomb/watcher_snapshot_test.go)，真实 SDK 的本地 HTTP 回归见 [sdk_snapshot_test.go](../../contrib/registry/servicecomb/sdk_snapshot_test.go)，原注册/查询/注销测试保留于 [registry_test.go](../../contrib/registry/servicecomb/registry_test.go)。

| 验收项 / 检查 | 结果与证据 |
|---|---|
| AC-01 | TestServiceCombSnapshotLifecycle 验证多节点完整列表、跳过 nil 实例、实例版本与请求服务名；修改 Metadata/Endpoints 后，客户端及其他快照不变 |
| AC-02 | 生命周期回归验证部分删除→空→恢复；TestServiceCombSnapshotEmptyAndErrors 覆盖首次为空、缺服务及包装错误映射、普通错误与非预期 ErrNotModified 保留 |
| AC-03 | 虚拟时钟验证 Watch 创建不查询、首个 Next 无等待、后续从 SDK 查询完成起间隔 1 秒；真实 SDK 测试核对两次 Next 对应两次查询 |
| AC-04 | TestServiceCombSnapshotCancelIntervalAndRepeatedStop 验证父 context、间隔取消及八个并发 Stop；TestServiceCombSnapshotStopDuringQuery 验证 Next 排队取消、SDK 返回后才报告取消；已取消的 Watch 创建返回 nil 接口和 context.Canceled |
| AC-05 | TestServiceCombSnapshotMultipleServicesAndSerialization 验证多服务监听独立、发现查询串行和排队取消；TestServiceCombSDKCompleteSnapshots 验证不发送 rev、相同 revision 仍返回完整列表、400012 转空和 503 错误保留 |
| AC-06 | 最终状态下模块全部测试、race、21 模块构建及独立审查通过；记录已同步 |
| 完整模块测试 | ServiceComb 模块 `go test -mod=readonly ./...`，七个顶层测试组通过，退出 0（2.132s） |
| race | ServiceComb 模块 `go test -mod=readonly -race ./...`，全部测试通过，退出 0（3.565s） |
| 模块构建 | ServiceComb 模块 `go build -mod=readonly ./...` 及最终根目录 `make check`（21 个模块）均退出 0 |
| 独立审查 | 已完成；生产实现、SDK 行为、测试及文档一致，无剩余可行动问题；取消创建返回 nil 接口的补充修复也已独立复核 |
| 格式与文档 | 六个变更 Go 文件通过 gofmt 核查，差异检查及追溯文件内容/本地链接检查通过 |

Go 验证统一设置 `GOCACHE=/tmp/meta-review-module-cache GOPROXY=off`。可控客户端测试使用 testing/synctest 虚拟时间，无新增依赖或生产测试钩子；真实 SDK 测试使用 httptest 临时回环 HTTP。首次默认环境禁止监听，允许临时回环监听后定向和完整测试均通过。构建有全局模块 stat 缓存不可写提示，不影响构建成功，无依赖变更。

真实 ServiceComb 联调未执行，未验证实际部署、鉴权和服务端通知。60 秒默认超时结论来自本地 sc-client v0.7.0 / foundation v0.4.0 源码核查，未执行耗时 60 秒的超时测试；在途取消行为通过受控阻塞客户端验证。独立审查为只读复核，分别记录测试证据，不视作真实部署验收。

## 边界与下一步

无实质范围偏差。每个 Registry 应独占 SDK client；已发出的 SDK 调用不能被单次 context 强制中断。核查发现 sc.Options.Timeout 未被传递，实际底层默认 HTTP 请求超时为 60 秒，已在需求/决策/README 澄清；不通过反射或全局配置改写该边界。SDK 地址健康检查及注册/注销与心跳生命周期维持原有实现。轮询有 1 秒等待和请求成本，无历史事件模型兼容。

需求、决策、实现、验收证据及审查状态已同步，用户确认后已提交为 `8445092`（`fix: poll complete ServiceComb discovery snapshots`）。用户已确认第 13 步 Eureka 方案，实施与验证见 [第 13 步进度](../eureka-concurrency/progress.md)。
