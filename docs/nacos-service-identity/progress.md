# 第 10 步实施与验证进度

## 当前状态

- [需求](requirements.md) 已确认，[设计选择](decision.md) 已记录。
- 实现、本地验证和独立审查已完成，用户已确认提交；基线为第 9 步提交 `8210fc3`。
- Level 2：注册数据及发现结果语义变化。
- 最后更新：2026-11-11。

## 实施记录

| 日期 | 工作 | 状态 |
|---|---|---|
| 2026-11-11 | 用户确认第 10 步 | 已确认 |
| 2026-11-11 | 核查本地 SDK 空列表及健康筛选语义 | SelectAllInstances + 共用转换方案确定 |
| 2026-11-11 | 注册元数据复制、必填 ID 校验及业务身份转换 | 已完成 |
| 2026-11-11 | Watch 复用 GetService，统一筛选和空快照语义 | 已完成 |
| 2026-11-11 | 默认运行的客户端替身回归及 integration 测试预期适配 | 已完成 |
| 2026-11-11 | 补齐 README、Gate/Game 示例及集成测试的 UpdateCacheWhenEmpty 配置 | 已完成 |
| 2026-11-11 | 本地验证、独立复核与追溯记录核对 | 已完成 |
| 2026-11-11 | 用户确认提交第 10 步 | 已确认，本记录随实现一并提交 |

## 验证与审查

测试代码见 [identity_test.go](../../contrib/registry/nacos/identity_test.go)，真实服务测试见 [registry_test.go](../../contrib/registry/nacos/registry_test.go)。

| 验收项 / 检查 | 结果与证据 |
|---|---|
| AC-01 | TestNacosIdentityRegistrationRoundTrip 覆盖 nil、空及已有元数据；TestNacosIdentityRejectsEmptyRegistrationID 验证空 ID 返回指定错误且不调用 SDK；注册未修改输入 map |
| AC-02 | 往返测试提供不同的 SDK InstanceId 和分组服务名，验证查询及监听仍返回应用 ID 与请求服务名 |
| AC-03 | TestNacosIdentityFiltersAndCopiesResults 与 TestNacosIdentityWatchMatchesQueryAndEmptyUpdates 验证分组、集群、健康/启用/权重筛选、协议回退、IPv6、版本和查询/监听一致性 |
| AC-04 | 覆盖首次 Next、nil 及非 nil 空列表更新、缺失/空 ID、无效权重；TestNacosIdentityPropagatesSDKErrors 验证查询、订阅及退订错误传播 |
| AC-05 | 修改返回值后 SDK 元数据及其他查询/监听结果保持独立；五组测试的 race 回归通过 |
| AC-06 | 默认测试、race、集成编译、21 模块构建及独立审查通过；记录已同步 |
| 默认单元测试 | 在 Nacos 模块执行 `go test -mod=readonly ./...`，退出 0；不依赖真实 Nacos |
| race | 在 Nacos 模块执行 `go test -mod=readonly -race -run TestNacosIdentity ./...`，退出 0，覆盖全部五组默认测试 |
| 集成测试编译 | `go test -mod=readonly -tags integration -run '^$' ./...` 退出 0；未执行真实服务测试 |
| 模块构建 | 根目录 `make check` 覆盖全部 21 个模块，退出 0 |
| 格式与文档 | 六个变更 Go 文件通过 gofmt 核查，差异检查及追溯文件内容、本地链接检查通过 |
| 独立审查 | 已完成；发现并补齐 SDK 清空缓存的配置前提，最终复核无剩余可行动问题 |

所有 Go 验证均使用 `GOCACHE=/tmp/meta-review-module-cache GOPROXY=off`。模块构建出现全局模块 stat 缓存不可写提示，但全部构建成功，无依赖变更。独立审查为只读复核，不将其计作真实 Nacos 联调。

## 边界与下一步

无实质范围偏差。审查补充的 `UpdateCacheWhenEmpty: true` 是空快照生效的必要配置，已同步仓库示例；外部客户端仍需自行启用，适配器无法通过 INamingClient 覆盖该配置。

真实 Nacos 服务未连接，未验证实际服务端通知及 SDK 缓存链路；保留集成测试供具备环境时显式执行。SDK 进行中的调用不能被 context 强制中断；旧数据缺 ID 时跳过，需应用重新注册，无历史兼容分支。

需求、决策、实现、验收证据与审查状态已同步，用户确认后已提交为 `8570d5f`（`fix: preserve application identity in Nacos discovery`）。用户已确认第 11 步 Consul 空快照方案，实施与验证见 [第 11 步进度](../consul-empty-snapshots/progress.md)。
