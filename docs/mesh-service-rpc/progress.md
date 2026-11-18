# 按服务名发送 RPC 的实施记录

本次新增公共 API，并调整共享服务发现的并发和关闭方式，按 Level 2 记录。[验收范围](requirements.md)。

## 调用与生命周期

- [RequestService](../../server/mesh/request_service.go) 从本地快照取一个具体 subject，再调用已有的 `Mesh.Request`。没有节点可选时可通过 `errors.Is(err, selector.ErrNoAvailableNode)` 判断。已知节点的有状态请求可以继续使用具体 subject。[调用示例](../../README.md#按服务名发送)
- Gate 和 Mesh 复用[服务发现实现](../../internal/discovery/resolver.go)，各实例各自管理缓存。每个目标服务只初始化一次并共享结果，失败后下次访问可重试。调用方取消会结束自己的等待。服务停止时取消共享查询，并等待监听和清理结束。
- 快照更新时构建实例到 subject 的映射。选点和地址读取使用同一份快照，监听异常或全部实例下线时使旧快照失效。在选点后、Broker 投递前离线的实例可能导致传输失败，发送端直接返回传输结果。

## 验证

- [Mesh 回归](../../server/mesh/request_service_test.go)覆盖同名首调合并、独立超时、发现期间停止、节点上下线、无效地址、缺少配置、截止时间透传和传输失败只发送一次。
- 既有 Gate 发现、监听恢复、绑定实例和关闭测试通过。根模块按约定跳过两项单进程多节点用例。Gate 和 Mesh 通过三轮竞态测试，game 示例测试通过，gate 示例编译通过。增量 lint 通过，`make check` 构建全部 21 个模块。
- 验证使用受控注册中心和内存 Broker，未运行真实注册中心的跨进程联调。已自查共享初始化、监听关闭与单次投递路径。

### 提交前自查

- [共享发现回归](../../internal/discovery/resolver_test.go)复现了最后一个等待者退出后旧查询仍占用缓存的问题。现在会移除尚未完成的缓存项并通知取消。即使适配器忽略取消、迟到返回，新快照仍可用。已发起的底层操作仍需适配器自行退出，关闭流程会等它结束。
- 选择器构造或监听器回调发生 panic 时，后台任务记录堆栈，释放已取得的监听。回归覆盖初始化失败后重试、监听异常后旧地址失效，以及关闭 panic 转为可检查的错误。
- [Mesh 回归](../../server/mesh/request_service_test.go)验证首次和重复调用 Stop 都能获得监听器的关闭错误，启动时会在订阅前拒绝通配符配置，首次发现全部地址无效时不会创建监听，选点中取消也不会向 Broker 发送。
- 根模块回归、21 个模块构建、增量 lint 及共享发现、Gate、Mesh 五轮竞态检查通过。按约定跳过两项单进程多节点用例。

## 缓存命中基准

[BenchmarkRequestService](../../server/mesh/request_service_test.go) 在 Apple M1、Go 1.27.1 下使用 `-cpu=1 -benchmem -count=5`。两组都复用 Mesh 上下文和 128 字节载荷，由内存 Broker 立即回复。服务名组使用已初始化的单实例快照，计时覆盖发送端选点、寻址和 Broker 调用，不含首次发现、网络与服务端处理。

| 入口 | 耗时中位数 | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 具体 subject | 252.6 ns/op | 480 | 8 |
| 服务名，缓存命中 | 341.9 ns/op | 480 | 8 |
