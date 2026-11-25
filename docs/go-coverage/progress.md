# 全仓库 Go 测试覆盖率进度

2026-11-25 在 `test/go-coverage` 完成补充测试并通过覆盖率验收。[验收要求](requirements.md)

| 验收项 | 验证结果 |
| --- | --- |
| AC-01 | 64 个包中，58 个具有可执行语句。仅默认套件就使这 58 个包的语句覆盖率均严格大于 95%，按语句数加权为 97.62%（5580 / 5716）。合并集成套件记录后为 97.94%（5598 / 5716）。覆盖率最低的包为 `pkg/host`，实际覆盖 59 / 62 条语句，即 95.16%。 |
| AC-02 | 用例检查协议编解码与描述符、日志命令退出、网络地址选择、会话与 RPC、选择器、注册中心重试和错误传播。示例启动代码、地址枚举和 ZooKeeper 连接通过包内参数或接口传入测试替身。生产入口沿用现有依赖和配置。 |
| AC-03 | 按模块用 `go test -mod=readonly -count=1 -covermode=count -coverprofile` 运行默认套件，21 个模块全部通过。Consul、Eureka 和 Nacos 同时通过 `integration` 套件。`make check` 构建 21 个模块，`golangci-lint fmt --diff` 无差异，`git diff --check` 通过。 |

根模块 Go lint 为 `0 issues`。受影响嵌套模块的 lint 仍报告与基线一致的 35 条已有问题，本次新增 0 条。集成环境使用临时服务，Consul 测试使用独立端口，Eureka 测试信任本地临时证书。云日志测试在隔离网络下运行。

后续清理了 Gate 客户端中 `SetWriteDeadline` 返回错误的不可达分支。Gorilla 在设置方法中保存期限，写入时应用期限，客户端继续通过 `WriteMessage` 处理失败。回环测试确认调用方更早的期限传到了底层连接。`client/gate` 覆盖率为 99.0%，其中 `writeLoop` 为 100.0%，`examples/gate-client` 为 100.0%。写入用例连续运行 5 次通过，根模块和示例的 Go lint 均为 `0 issues`。
