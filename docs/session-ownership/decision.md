# 会话身份与条件释放

## 状态与约束

2026-11-11，已实施并完成本地验证和独立复核，用户已确认提交。用户确认 [需求](requirements.md)，代码简洁优先、无需历史兼容。

## 方案比较与选择

仅将 Redis 解绑改为 Lua、只比较节点 ID，仍不能区分同一节点的两次连接。使用全局递增代次需要额外计数与永久保留规则；本步骤仅需身份等值比较，不需要时间排序。因此采用每次连接生成的新随机 ID，使用标准库 crypto/rand.Text，无新运行时依赖。

通过 SessionLocator 扩展原 Locator，提供 Claim、Release 和 Session。Session 包含 ID 和 Node；Node 为空且 ID 存在表示最后会话已离线。普通业务定位接口保持独立用途，避免把网关连接标识暴露到业务节点 ID 中。

Redis 在同一个用户 Hash 中使用 node:<service> 与 session:<service> 字段；Claim/Release 使用单键 Lua，Session 使用一次 HMGET，避免多键 Redis Cluster 槽位问题。Release 只删除路由字段、保留身份；Node 和 AllNodes 仅返回路由。旧字段格式不读取。

独立审查发现 go-redis 的 ReadOnly、RouteByLatency 和 RouteRandomly 可将 HMGET 路由到滞后副本。定位器构造时统一关闭这三个选项，确保正常 Cluster/Sentinel 拓扑下从主节点读取当前归属。普通 Bind/UnBind 在发现会话字段时返回错误，避免绕过会话身份校验。Session 查询没有会话 ID 的普通绑定时返回零值，普通路由继续由 Node 获取。

Gate 按用户加锁，覆盖 Claim、本地替换、事件发出和断开 Release。锁只对同用户串行化，引用计数在无使用者时回收，避免永久积累本地锁。不同 Gate 的竞争由 Redis 原子写入决定；不会创建跨进程锁。旧连接关闭由已有断开回调完成；失败的新连接从未进入本地会话表。

Mesh 每次消费状态事件读取当前会话，不再仅凭 UID 处理。仅比对身份与在线状态，不添加消费者缓存。没有业务回调的事件直接跳过。此检查不能与任意业务回调形成跨系统原子事务，不作相应保证。

## 影响与回退

Gate/Mesh 配置要求实现 SessionLocator；所有生产实现、示例和测试一并更新。Redis 数据格式和事件头直接切换，部署需同版本组件协同；旧 Hash 数据不再作为有效位置读取。仅可整体回退代码与数据格式，不能混跑旧事件消费者。

离线标识保留至业务数据清理或下一次 Claim 覆盖；当前不添加 TTL，否则必须为仍可能到达的旧事件另设判定依据。事件发布失败继续返回/记录错误，不在本步骤追加可靠投递基础设施。
