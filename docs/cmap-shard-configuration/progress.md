# 第 16 步实施与验证进度

## 当前状态

- [需求与设计选择](requirements.md) 已确认；实现、本地验证及独立审查完成，用户已确认提交。
- 基线为第 15 步提交 `298b5f8`；Level 2，构造接口及并发配置归属变化。
- 最后更新：2026-11-11。

## 实施与验证

| 工作 | 状态 / 证据 |
|---|---|
| 用户确认第 16 步方案 | 已确认 |
| 根因、调用点及规则核查 | 完成，无适用 AGENTS 文件或仓内生产调用点 |
| 单一构造入口与实例分片长度 | 实现完成，无冗余分片数字段或旧接口包装 |
| 现有测试、基准及用法迁移 | 完成，测试辅助构造只在测试代码中共享，基准显式传分片数 |
| 多分片实例及并发回归 | 三组新增回归完成 |
| 普通/race、基准冒烟、21 模块构建 | 全部通过，证据见下表 |
| 独立审查 | 已完成，无遗留可行动问题 |
| 文档及链接核查 | 四个 Go 文件格式、差异检查及五份实质文档 / 19 个本地链接检查通过 |
| 用户确认提交第 16 步 | 已确认，本记录随实现一并提交，完成原 16 步计划 |

## 验收证据

实现见 [cmap.go](../../pkg/cmap/cmap.go)，旧测试迁移见 [cmap_test.go](../../pkg/cmap/cmap_test.go)，新增回归见 [cmap_shards_test.go](../../pkg/cmap/cmap_shards_test.go)，基准迁移见 [cmap_bench_test.go](../../pkg/cmap/cmap_bench_test.go)，用法见 [README](../../pkg/cmap/README.md)。

| 验收项 / 检查 | 结果与证据 |
|---|---|
| AC-01 | TestShardConfigurationValidation 验证 0/-1/MinInt 分片数及 nil hash 返回 nil/error；常量保持 32，其余回归使用正常正分片数 |
| AC-02 | TestShardConfigurationsStayIndependent 先后构造并填充 1/3/32/64 分片实例，使用包括 -1 大哈希在内的 97 个键，检查创建其他实例后原有定位、Count、Has 和数据完整 |
| AC-03 | 同一回归逐实例检查 Keys/Iter/Items/JSON，清空并重新反序列化一个实例时其余实例不变 |
| AC-04 | TestShardConfigurationsConcurrent 运行四个并行子测试，各有四个写入者和一个枚举者，交错 Set/Get/Remove 与 Keys/Iter/Count；仅在写入结束后断言完整集合，race 通过 |
| AC-05 | 包级完整普通/race、三类基准冒烟、全部模块构建及独立审查通过，文档已同步 |
| 普通测试 | `go test -mod=readonly -timeout=60s ./pkg/cmap`，全部 33 个顶层测试组通过，退出 0（0.286s） |
| race | `go test -mod=readonly -race -timeout=60s ./pkg/cmap`，完整测试通过，退出 0（1.641s） |
| 基准冒烟 | MultiInsertDifferent、MultiGetSetDifferent、MultiGetSetBlock 三类基准共 15 个场景通过，退出 0（0.318s），完整命令见下方 |
| 模块构建 | 根目录 `make check`，全部 21 个模块通过，退出 0 |
| 独立审查 | 只读核对实例分片定位、计数、遍历等待数、非法构造及测试/基准迁移，未发现遗漏生产调用点或遗留可行动问题；未重复运行检查 |

验证统一使用 `GOCACHE=/tmp/meta-review-module-cache GOPROXY=off`。基准仅执行每场景一次以确认迁移后能运行，未据此作性能优劣或分片数选型结论。未新增依赖或连接外部服务。

```sh
go test -mod=readonly -run '^$' -bench 'BenchmarkMulti(InsertDifferent|GetSetDifferent|GetSetBlock)$' -benchtime=1x -count=1 -timeout=60s ./pkg/cmap
```

## 边界与下一步

无实质范围偏差。验证限定为 cmap 行为、基准可运行及全模块构建；未执行真实业务压力测试、性能选型或外部基础设施联调，因为本步目标为分片配置隔离。保留现有跨分片弱一致性和锁内回调边界，零值初始化和回调 panic 行为不在本步扩展范围。

需求、设计选择、实现、验收及审查状态已同步，原计划导航也已更新。原 16 步方案均已实现，前 15 步已分别提交；用户已确认将第 16 步实现与记录一并提交，完成原计划。各步骤未执行的真实联调及验证边界继续以对应记录为准。
