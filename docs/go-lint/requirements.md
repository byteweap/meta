# Go 格式与静态检查修复

本次修复 `dev` 上根模块和游戏示例的已报告告警，以及仓库范围的 Go 格式差异。改动涉及多个 Go 模块，采用 Level 2 追溯。

| 验收项 | 要求 |
| --- | --- |
| AC-01 | 根模块和 `examples/game` 的 `golangci-lint run ./...` 均通过。保留现有 lint 配置。 |
| AC-02 | 从仓库根目录运行 `golangci-lint fmt --diff` 时没有格式差异。 |
| AC-03 | 保留 Gate 的行为、游戏示例的路由和消息契约。根模块和游戏示例测试以及 `make check` 通过。 |

实施和验证见 [进度记录](progress.md)。
