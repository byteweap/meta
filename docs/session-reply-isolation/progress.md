# 回包隔离实施与验证

本地实施和验证完成。范围、验收标准见 [需求](requirements.md)，滚动升级与回退见 [决策](decision.md)。

| 验收项 | 实现与验证 |
|---|---|
| AC-01 | [Gate 原始帧测试](../../server/gate/gate_envelope_test.go)验证会话 ID 来自收包连接而非当前 UID 映射，发布数据不变。[Mesh 回包测试](../../server/mesh/context_error_test.go)验证成功、失败和复制上下文保留回包元数据，也覆盖旧 Gate 缺少会话 ID 的路径。 |
| AC-02 | [Gate 重连测试](../../server/gate/gate_session_ownership_test.go)使用真实 WebSocket、实际 Mesh handler 和内存 Broker：两条连接发送相同序号，旧成功和失败响应均不进入新连接，新响应及显式推送照常到达。 |
| AC-03 | 同一组 Gate 测试验证兼容模式、严格模式、标记缺失、会话不匹配和显式推送；[NATS 模块测试](../../contrib/broker/nats/broker_test.go)验证内部头经过本机 NATS 发布订阅原样往返。 |
| AC-04 | [Go 客户端测试](../../client/gate/client_test.go)覆盖乱序响应、推送、取消、超时、断开时清理在途请求和两种序号分配方式。[客户端示例](../../examples/gate-client/cmd/app.go)为请求设置超时，不重放。 |

根模块测试、客户端/Gate/Mesh 竞态检查、关键回归重复 5 次、NATS 和游戏模块测试、客户端示例编译、21 个模块的 `make check`、`gofmt` 和 `git diff --check` 均通过。使用 `golangci-lint 2.15.0` 对客户端、Gate、Mesh 和集群元数据包运行增量检查，修复三处测试代码告警后报告 0 个问题；客户端和 Gate 的竞态复测通过。未连接生产 Broker 或执行滚动上线。严格模式保持默认关闭，需由部署方在确认自定义发布者迁移完成后启用。
