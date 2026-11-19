# Gate 运行租约实施与验证

基线为 `66d7780`。用户确认把逐连接租约替换为每个 Gate 一份租约。[验收范围](requirements.md) · [实现决策](decision.md)

- [Gate 运行租约](../../server/gate/gate_lease.go)在就绪前建立，60 秒到期，每 15 秒续期一次。暂时的存储错误在剩余有效期内重试。Redis 明确拒绝续期，或最后一次确认的有效期耗尽时，Gate 启动停止流程。
- [Redis 定位器](../../contrib/locator/redis/locator.go)使用 `SET NX` 建立租约、`PEXPIRE` 延长。会话绑定保存本次 Gate 运行 ID，`Session` 检查对应的租约。普通位置读取恢复为单次 `HGET` 或 `HGETALL`。
- [定位器接口](../../component/locator/locator.go)新增 `AcquireGateLease` 和 `RenewGateLease`，`Claim` 接收运行 ID。自定义 `SessionLocator` 需要同步实现。读到旧格式会话时保留最后 ID，节点按离线返回，重新连接后写入运行 ID。

## 自查

- 控制旧认领通过租约检查后暂停。在原租约失效、新连接写入后恢复旧认领，修复前会覆盖新会话。现在把旧会话 ID 的比较放进写入脚本，冲突时重新读取并验证租约。[认领回归](../../contrib/locator/redis/session_lease_test.go)
- 使用虚拟时间控制持续的续期失败，修复前 Gate 会在最后一次有效期之后继续服务。现在后续续期等待受该期限约束，到点进入关闭流程。用例同时验证成功续期会延长本地期限。[Gate 回归](../../server/gate/gate_lease_test.go)

## 验收证据

- [Gate 生命周期回归](../../server/gate/gate_lease_test.go)验证租约创建失败时保持未就绪，虚拟时间下每轮只续期一次，暂时的存储错误允许重试，明确的租约丢失触发停止。
- [Redis 会话回归](../../contrib/locator/redis/session_lease_test.go)验证同一次运行的两个用户随同一个键到期，过期键无法续期，重用应用 ID 时旧会话保持离线，迟到释放无法删除新归属，普通绑定可继续读取，存储错误会返回给调用方。
- 单个 Gate 连接实际 Redis 7.2，建立两个 WebSocket 连接并确认只存在一份运行租约。观测到该键的截止时间自动延长，正常停服后两个会话均已释放。另以单个 Mesh 验证同一运行租约到期后两个 UID 均返回 `ErrUserOffline`，复用应用 ID 时只有重新认领的用户恢复推送。实际 Redis 专项用例通过竞态检查。
- 根模块测试按约定跳过两项单进程多节点用例。审查后 Gate、Mesh 和 Redis 定位器分别通过五轮竞态检查，Gate 租约专项通过五十轮竞态检查。Gate 和 game 示例此前已验证，审查后重新通过 `make check` 的 21 个模块构建。全量 lint 仅报告基线已有的 Gate 8 项和 Redis 1 项。

## 开销与边界

每个 Gate 每轮续期执行一次 Redis `PEXPIRE`，开销与本地连接数无关。已绑定的普通业务查询仍执行一次 `HGET`，在线会话查询执行一次 `HMGET` 和一次 `EXISTS`。新连接认领增加一次 `HGET` 读取会话 ID。各命令可分别路由到不同分片。此次实际 Redis 验证使用单实例。

单个连接断开时，`Release` 若失败，存活的 Gate 租约仍可能使旧归属可见，直到用户重连或该运行的租约失效。已到期的运行键由 Redis 清理，用户 Hash 的最后会话 ID 保留。依赖实时在线状态的调用使用 `Session`，位置接口返回记录中的归属。
