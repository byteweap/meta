# 第 14 步实施与验证进度

## 当前状态

- [需求](requirements.md) 已确认，[决策](decision.md) 已记录；实现、本地验证及独立审查完成，用户已确认提交。
- 基线为第 13 步提交 `582e726`；Level 2 + 决策记录。
- 最后更新：2026-11-11。

## 实施与验证

| 工作 | 状态 / 证据 |
|---|---|
| 用户确认第 14 步四项方案 | 已确认 |
| 本地 SDK 与原实现核查 | 完成，确认默认 TTL、地址键、Existed 和托管心跳限制 |
| 注册/注销、选项及身份转换 | 实现完成，七组注册/发现回归通过 |
| 监听内部 SDK ID 缓存与回归 | 实现完成，五组监听回归通过 |
| 普通测试、race、21 模块构建 | 全部通过，证据见下表 |
| 独立审查 | 已完成，无剩余可行动问题 |
| 文档及链接核查 | 五个 Go 文件格式、差异检查及五份实质文档 / 19 个本地链接检查通过 |
| 用户确认提交第 14 步 | 已确认，本记录随实现一并提交 |

## 验收证据

注册与发现测试见 [registry_test.go](../../contrib/registry/polaris/registry_test.go)，监听测试见 [watcher_test.go](../../contrib/registry/polaris/watcher_test.go)。生产实现见 [registry.go](../../contrib/registry/polaris/registry.go)、[option.go](../../contrib/registry/polaris/option.go)、[watcher.go](../../contrib/registry/polaris/watcher.go)，用法与限制见 [README](../../contrib/registry/polaris/README.md)。

| 验收项 / 检查 | 结果与证据 |
|---|---|
| AC-01 | TestPolarisRegistrationIdentityAndDefaults 验证原业务 ID/服务名、多 endpoint、IPv6、元数据和 Version 隔离、按地址注销 |
| AC-02 | TestPolarisHeartbeatAndOptionalConfiguration 验证 SDK 方法选择、显式 TTL/超时/零重试；默认值测试调用真实 SDK Validate/SetDefaultTTL；ConcurrentRequestsDoNotShareOptions 验证 24 个并发调用中的请求参数独立 |
| AC-03 | TestPolarisValidatesBeforeSideEffects 覆盖 22 类非法输入及 nil/预取消；RegistrationRollback 覆盖部分失败/取消只清理新建并保留清理错误；DeregisterValidationAndFailures 验证预校验、继续注销和错误聚合 |
| AC-04 | TestPolarisDiscoveryIdentityFilteringAndIsolation 验证业务身份、过滤、空结果、错误与快照隔离；WatcherSDKIdentityUpdates 验证同业务 ID 两个 SDK ID 的独立增删、恢复健康和隔离；其余 watcher 测试覆盖首空/EOF、创建错误、取消和并发 Stop/Next |
| AC-05 | 普通测试、race 及全部模块构建通过；独立审查和追溯核对完成 |
| 普通测试 | Polaris 模块 `go test -mod=readonly ./...`，12 个顶层测试组通过，退出 0（0.659s） |
| race | Polaris 模块 `go test -mod=readonly -race ./...`，全部默认测试通过，退出 0（1.442s） |
| 模块构建 | 根目录 `make check`，全部 21 个模块通过，退出 0 |
| 独立审查 | 只读核对实现、12 组回归及真实 SDK 源码；默认值、托管心跳入口、按地址注销、仅回滚新建实例及监听身份转换均一致，无剩余可行动问题 |

以上命令统一使用 `GOCACHE=/tmp/meta-review-module-cache GOPROXY=off`。测试使用可控 SDK Provider/Consumer 替身及真实 SDK 校验方法，不创建网络监听，不连接外部服务，不新增依赖。局部专项结果与最终完整测试分开，表中记录最终完整测试证据。

## 边界与下一步

真实 Polaris 服务联调未执行，未验证真实服务器注册响应和托管心跳运行。SDK 服务端版本要求、无 context 请求接口、取消不等待在途调用，以及现有 WatchService 共享通道/查询订阅间隙/丢增量/无取消订阅的边界见 [需求](requirements.md)。未扩展 SDK 订阅架构，没有历史兼容或新增依赖。

需求、决策、实现、验收证据及审查状态已同步，无实质范围偏差。第 14 步已提交为 `3c4d3d6`。用户已确认 [第 15 步座位空间方案](../seat-space-bounds/requirements.md)，后续实施与验证见该主题记录。
