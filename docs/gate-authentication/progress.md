# 第 3 步实施与验证进度

## 当前状态

- 需求见 [网关握手前身份验证](requirements.md)。
- 第 3 步已提交为 `0d0a8a1`，实施、本地验证和独立审查均已完成。第 4 步已获确认，进度见 [应用生命周期](../application-lifecycle/progress.md)。
- 追溯级别为 Level 2，原因是涉及身份验证边界。
- 基线提交为 `3c23403`。
- 最后更新为 2026-11-11。

## 实施计划与记录

| 日期 | 工作 | 状态 |
|---|---|---|
| 2026-11-11 | 用户确认第 3 步方案 | 已确认 |
| 2026-11-11 | 建立需求和进度记录 | 已创建 |
| 2026-11-11 | 必填身份验证、握手前校验及会话传值 | 实现完成 |
| 2026-11-11 | 更新 README 和网关示例 | 已完成，无默认令牌或请求 uid 回退 |
| 2026-11-11 | 全模块构建 | 21 个模块全部通过 |
| 2026-11-11 | 身份验证回归测试 | 已完成，覆盖握手、可信 ID、配置边界和已有会话保留 |
| 2026-11-11 | 网关完整测试及认证数据竞争检查 | 均通过 |
| 2026-11-11 | 独立代码审查 | 已完成，生产实现、示例、文档和测试无未解决发现 |
| 2026-11-11 | 用户批准第 3 步提交 | 已确认 |

## 验证证据

| 标准或检查 | 状态 | 证据或待办 |
|---|---|---|
| AC-01 | 已通过 | `TestGateAuthRequiresExtractor` 验证缺失、nil、显式清空均拒绝启动，且未创建监听器；既有 `TestGateValidate` 验证完整配置通过 |
| AC-02 | 已通过 | `TestGateAuthRejectsInvalidIdentityBeforeUpgrade` 覆盖 0 和负数，伪造 `uid=42` 仍返回 401；原会话保留，绑定、解绑和事件发布次数为 0 |
| AC-03 | 已通过 | `TestGateAuthUsesVerifiedIdentityOnce` 使用真实本地 WebSocket 握手，认证返回 42、查询参数传入 99，断言会话和定位绑定均使用 42，且验证函数仅调用一次 |
| AC-04 | 已通过 | `TestGateAuthSkipsUnknownPath` 断言错误路径返回 404，验证函数未调用 |
| AC-05 | 已通过 | 示例检查完成；`GOCACHE=/tmp/meta-review-module-cache GOPROXY=off make check` 覆盖 21 个模块，退出码 0 |
| AC-06 | 已通过 | `GOCACHE=/tmp/meta-review-module-cache GOPROXY=off go test -mod=readonly ./server/gate` 和同环境的 `go test -mod=readonly -race ./server/gate -run 'TestGateAuth'` 均退出 0 |
| 代码审查 | 已完成 | 独立审查生产实现、示例、README、既有测试调整及新增认证测试，无未解决发现 |
| 文档和差异 | 已通过 | 6 份追溯文档及其本地链接检查通过，`git diff --check` 退出码 0；修改的 Go 文件均已格式化 |

新增测试见 [gate_auth_test.go](../../server/gate/gate_auth_test.go)，既有绑定失败回滚测试继续通过。首次运行认证测试时，本地连接受到监听权限限制；获准重新运行后，完整网关测试和认证 race 检查均通过。

## 偏差、风险与剩余工作

当前没有范围偏差。凭据验证仍由业务实现，框架负责强制配置并在握手前执行。真实基础设施和身份服务不在本步骤的本地验证范围内。

全模块构建中出现模块 stat 缓存目录不可写提示，构建仍全部成功，未修改依赖声明或校验文件。

## 完成范围与下一步

生产变更集中在 [网关配置](../../server/gate/option.go)、[启动校验与 HTTP 处理](../../server/gate/gate.go)、[连接回调](../../server/gate/gate_handler.go) 和 [配置错误](../../errors/errors.go)。同步更新 [网关示例](../../examples/gate/cmd/main.go)、[README](../../README.md)、网关测试，以及第 2 步的提交状态与后续链接。没有新增依赖或兼容分支。

需求、实施、验收证据和审查状态已同步。第 3 步已提交为 `0d0a8a1`，提交说明为 `fix: authenticate gate requests before websocket upgrade`。后续从 [第 4 步进度](../application-lifecycle/progress.md) 继续，第 5 至 16 步待确认。
