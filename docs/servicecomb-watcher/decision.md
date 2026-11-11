# 用 Next 驱动完整查询

## 状态

2026-11-11，[需求](requirements.md) 已确认；实现、本地验证及独立审查已完成，用户已确认提交。证据见 [实施与验证](progress.md)。

## 选择与依据

本地 sc-client v0.7.0 的 WatchMicroService 为同一 consumer ID 只设置第一个回调，方法返回后自行运行读取协程。Close 关闭所有连接，读取退出后仍进入重连，没有单个 watcher 的取消句柄。继续补事件缓存或共享事件分发仍无法解决客户端连接生命周期，且显著增加状态。

因此按已确认方案改用 Next 驱动完整查询：创建 Watch 不发请求，首次 Next 立即查询，后续从上一轮查询完成起等待 1 秒。仅调用 Next 的协程执行查询，不另开后台 worker，不维护增量缓存、事件队列或可关闭的数据通道。当前服务列表自然反映增加、更新和删除，完整空列表清除旧路由。

GetService 和 Watch 共用转换。使用 sc.WithoutRevision() 请求完整响应；SDK 即使忽略请求 revision 仍会读取和更新 client.revision，因此 Registry 用容量为 1 的通道串行查询并支持排队取消。Watcher 同样串行 Next 以保护轮询时间。context 在进入 SDK 前及返回后检查；不为伪造强制取消而增加可能遗留的查询 goroutine。

成功结果复制 Metadata 与 Endpoints，并使用 instance.Version；已知 ErrMicroServiceNotExists 等价于正常空列表，其余错误保留。删除仅为旧事件监听服务的 Put、接口方法要求与全局消费者 ID。

## 影响与回退

增加约 1 秒轮询等待及每次 Next 的完整查询成本，但得到明确的快照和停止行为。每个 Registry 应独占 SDK client；内部串行门不覆盖调用方绕过 Registry 的并发 SDK 查询。停止等待可以立即完成，已经进入 SDK 的请求仍需返回或超时。

核查发现 sc-client v0.7.0 未将 Options.Timeout 传给底层客户端；foundation v0.4.0 默认 HTTP 请求超时为 60 秒（全局默认未被外部修改时）。本步骤不通过改全局配置或反射覆盖 SDK 内部状态。自定义 RegistryClient 需保证查询有限期返回。SDK NewClient 自身启动地址健康检查协程，此既有客户端资源不属于适配器新增监听协程。

不升级 SDK，不处理注册心跳生命周期，不保留旧事件 API 兼容。若将来采用支持单独取消的推送 SDK，应整体替换发现机制并重新验证完整快照、隔离、错误和停止语义，不恢复原单事件作为完整列表的行为。
