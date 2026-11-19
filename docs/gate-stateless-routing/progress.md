# Gate 无状态转发实施记录

[验收范围](requirements.md) · [设计决策](decision.md)

- 请求链路观测 `d3ef72d` 已快进合入本地 `dev`。无状态转发分支从该提交创建。
- 本次接入服务发现已有元数据，缓存快照更新时计算无状态声明，避免逐请求扫描实例。
- [自审](../../internal/discovery/resolver.go)覆盖请求与快照更新并发、重复实例声明冲突、选择器更新失败。地址与声明在同一把锁内提交。更新发生 panic 时两者一并失效。
- [Gate 专项](../../server/gate/gate_stateless_test.go)用 `stateless=true` 验证同实例元数据切换、UID 选点、定位调用次数、选点失败、绑定实例下线和服务下线。[Resolver 专项](../../internal/discovery/resolver_test.go)验证有效实例过滤、声明值不一致、重复 ID 和选择器更新失败。专项竞态检查通过二十轮。
- 根模块测试、Registry、Resolver、Gate 和 Mesh 三轮竞态检查、Redis Locator 模块竞态检查及 `make check` 的 21 个模块构建通过。按约定跳过两个单进程多节点用例。变更范围 lint 为零问题，Gate 全包 lint 仍报告 8 个基线问题。

## 复审修复

- 独立复现确认 ServiceComb 注册时丢弃业务元数据，且按 UID 选点的自定义选择器会拒绝空 key。
- [ServiceComb](../../contrib/registry/servicecomb/registry.go)注册时复制业务元数据，并写入保留字段。[Gate](../../server/gate/gate_message.go)在无状态路径以 UID 选点，选择器调用前会在快照锁内再次校验声明。
- 两个原始复现和对应回归通过十轮竞态检查。回归覆盖 ServiceComb 注册后查询业务元数据和保留字段，以及 Gate 服务切换模式后使用原有选择器。ServiceComb 模块竞态测试、根模块测试、变更范围 lint 和 21 个模块构建通过。

## 本地转发基准

Apple M1、Go 1.27.1，合并基线与复审修复后的版本交替运行五轮，每轮单核 400 毫秒。输入为 128 字节业务载荷，发现快照预热，Broker 使用内存适配器。Redis 项使用仓库的 Redis Locator 经 TCP 连接回环 miniredis，测试 UID 为 42424242 且未绑定业务节点。命令数取预热后 Redis 端增量。

| 路径 | 基线中位数 | 当前中位数 | Redis 命令/次 | 分配/次 |
| --- | ---: | ---: | ---: | ---: |
| 已绑定，内存定位器 | 776.5 ns | 799.4 ns | — | 996 B，20 次 |
| 未声明，Redis 定位器 | 29.21 µs | 27.55 µs | 1 → 1 | 1808 B，67 次 |
| 声明无状态，Redis 定位器 | 29.11 µs | 0.86 µs | 1 → 0 | 1808 B，67 次 → 1032 B，24 次 |

耗时范围为 Gate 外层帧解码到内存 Broker 接受发布。生产网络与 Broker 耗时需用实际部署中的 `Routing` 与服务指标评估。
