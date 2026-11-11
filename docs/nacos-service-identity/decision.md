# 共用查询与业务身份

## 状态

2026-11-11，[需求](requirements.md) 已确认；实现、本地验证和独立审查完成，用户已确认提交。证据见 [实施与验证](progress.md)。

## 选择与依据

Nacos SDK v1.1.6 的 SelectInstances 会把空 Hosts 作为错误；SelectAllInstances 可正常返回空列表。因此采用 SelectAllInstances，再在一个入口中过滤健康、启用和正有限权重，转换应用身份、原始服务名及复制后的元数据。保留权重向上取整语义，使用浮点格式化避免转 int64 溢出。

审查本地 SDK 发现，默认 `UpdateCacheWhenEmpty: false` 会在已有缓存时忽略服务端空 Hosts。适配器的输入是已创建的 INamingClient，无法从接口覆盖配置。因此要求初始化时显式设为 true，并同步 README、Gate/Game 示例及集成测试。空快照保证依赖 SDK 实际提供空状态；不把客户端替身测试视为真实 SDK 缓存机制已联调。

Watcher 持有 Registry，收到通知后直接调用其 GetService，删除重复的查询参数与转换逻辑。两条路径自然共享 Group/Cluster、筛选和字段语义，不增加转换策略接口。

仅接受 Metadata[id] 作为业务身份。缺少 ID 的外来或旧实例跳过并记录，避免一个无效实例使整个有效列表失败，也避免回退后生成错误路由。注册提前拒绝空 ID，防止框架自身继续写入不可发现的实例。

原有 registry_test.go 固定连接本地 Nacos，调整为 integration 构建标签并同步其过时预期。新增默认运行的客户端替身测试，直接覆盖注册、查询和监听；真实集成仅在明确启用标签并提供 Nacos 时执行。

## 影响与回退

公共组件接口与 SDK 版本不变。旧注册数据缺少应用 ID 时不会用于路由，需要应用重新注册；不提供历史格式转换。回退需整体回退适配器行为，不能把 Nacos InstanceId 当作新业务 ID 混用。
