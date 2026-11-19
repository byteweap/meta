# ServiceComb Registry

本包实现 `component/registry` 的 ServiceComb 适配器。

**特性**
- 支持服务注册、发现与 Watch
- Watch 由 Next 驱动完整快照查询，支持空列表及取消
- 支持心跳上报
- 使用环境变量设置 App 与环境

**安装**

```bash
go get github.com/byteweap/meta/contrib/registry/servicecomb
```

**最小用法**

```go
package main

import (
  "context"

  "github.com/byteweap/meta/component/registry"
  sreg "github.com/byteweap/meta/contrib/registry/servicecomb"
  sc "github.com/go-chassis/sc-client"
)

func main() {
  client, err := sc.NewClient(sc.Options{Endpoints: []string{"127.0.0.1:30100"}})
  if err != nil {
    panic(err)
  }
  reg := sreg.NewRegistry(client)

  svc := &registry.ServiceInstance{
    ID:        "node-1",
    Name:      "game",
    Version:   "v1",
    Endpoints: []string{"rest://127.0.0.1:9000"},
  }

  _ = reg.Register(context.Background(), svc)
  _ = reg.Deregister(context.Background(), svc)
}
```

**注意事项**
- 通过环境变量设置 App 与环境：`CAS_APPLICATION_NAME`、`CAS_ENVIRONMENT_ID`
- `Register` 会处理“服务已存在”的场景并生成实例 ID（若为空）
- 注册时将 `ServiceInstance.Metadata` 复制到实例属性。`appId` 和 `environment` 由适配器设置。
- Watch 创建时不发请求，首次 `Next` 立即查询完整列表，后续从上一轮查询完成起等待 1 秒。无 `Next` 调用时不轮询；结果可能重复，不保证逐个历史状态都被消费。
- 查询与监听共用转换：ID 使用实例 ID，Name 使用请求的服务名，Version 使用实例版本，Metadata 和 Endpoints 独立复制。成功空列表及 SDK 明确的服务不存在错误都返回正常空快照，其余错误保留。
- 发现查询使用 `sc.WithoutRevision()` 获取完整数据。同一 Registry 内的查询串行执行，避免 SDK 共享 revision 发生竞争；每个 Registry 应独占 SDK client，避免外部并发访问其发现接口。
- `Stop` 仅取消该监听，可并发重复调用；轮询等待及查询排队可立即取消。SDK 在途调用不能强制中断，返回后会报告 context 错误。
- 当前 sc-client v0.7.0 未将 `sc.Options.Timeout` 传给底层 HTTP 客户端，未修改全局默认值时实际请求超时为 60 秒。不要依赖该字段缩短在途取消时间；自定义 RegistryClient 也应保证查询能有限期返回。
- 适配器不再创建后台监听协程；SDK 客户端自身的地址健康检查协程仍存在。注册心跳生命周期不属于本次监听修改范围。
- 已移除测试专用 `Watcher.Put` 及 `RegistryClient.WatchMicroService` 要求，不提供旧事件模型兼容。

**验证**

默认测试使用可控客户端和真实 SDK 配本地 HTTP 替身，不需要真实 ServiceComb 服务：

```bash
go test -mod=readonly ./...
go test -mod=readonly -race ./...
```
