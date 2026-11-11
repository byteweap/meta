# Polaris Registry

本包实现 `component/registry` 的 Polaris 适配器。

**特性**
- 支持配置命名空间、Token、权重、TTL 等
- 支持心跳上报与实例隔离
- 支持 Watch 服务变更

**安装**

```bash
go get github.com/byteweap/meta/contrib/registry/polaris
```

**最小用法**

```go
package main

import (
  "context"

  "github.com/byteweap/meta/component/registry"
  preg "github.com/byteweap/meta/contrib/registry/polaris"
  "github.com/polarismesh/polaris-go/api"
)

func main() {
  conf := api.NewConfiguration()
  conf.GetGlobal().GetServerConnector().SetAddresses([]string{"127.0.0.1:8091"})
  reg := preg.NewRegistryWithConfig(conf, preg.WithNamespace("default"))

  svc := &registry.ServiceInstance{
    ID:        "node-1",
    Name:      "game",
    Version:   "v1",
    Endpoints: []string{"grpc://127.0.0.1:9000"},
  }

  if err := reg.Register(context.Background(), svc); err != nil {
    panic(err)
  }
  // 业务运行结束后注销。
  if err := reg.Deregister(context.Background(), svc); err != nil {
    panic(err)
  }
}
```

**配置**
- `WithNamespace` 设置命名空间
- `WithServiceToken` 设置服务访问 Token
- `WithProtocol` 设置协议类型
- `WithWeight` 设置权重，默认 100，范围 0–10000
- `WithHealthy` 设置健康状态
- `WithIsolate` 设置隔离
- `WithTTL` 设置心跳 TTL（秒），必须为正；未设置时 SDK 使用 5 秒
- `WithTimeout` 设置正数请求超时，未设置时使用 SDK 配置
- `WithRetryCount` 设置非负重试次数，支持显式 0；未设置时使用 SDK 配置
- `WithHeartbeat` 设置是否启用 SDK 托管心跳，默认启用；禁用时忽略 TTL

**注意事项**
- 保留 `ServiceInstance.ID` 和原始 `Name`，查询与监听使用原服务名。元数据的 `id`、`kind`、`version` 由适配器写入，不修改调用方数据，不兼容旧服务名或 SDK ID 格式。
- endpoint 必须为 `scheme://host:port`，支持 IPv6，不接受凭据、路径、查询或片段。同一服务不能重复注册相同 host/port，即使协议不同。
- 全部地址及请求先校验再注册；部分失败仅尽力清理本次新建实例，保留预存实例，并汇总注册及清理错误。SDK 超时可能发生在远端已处理请求之后，不提供远端事务保证。
- 注销按 namespace/service/host/port 定位并取消对应 SDK 心跳任务，无需拆分或保存 SDK ID。单项注销失败仍尝试其他地址；context 取消后停止后续请求。
- 托管心跳要求 Polaris Server ≥1.10。SDK 接口不接受 context；适配器在请求前后检查取消，无法中断在途请求。SDK 注销只取消后台任务，不等待正在执行的心跳或重注册退出。
- 查询和监听仅返回健康、未隔离、权重为正且包含 `id/kind` 的实例，返回元数据独立于 SDK 缓存。监听内部按 SDK ID 更新，对外返回业务 ID。
- 当前 SDK `WatchService` 为同服务共享增量通道，初始查询与订阅有间隙，不提供取消订阅，缓冲区满时可能丢弃事件。`Stop` 只取消本地等待；本步未重构 SDK 订阅方式或提供丢事件恢复。

本步设计、验证和已知边界见 [变更记录](../../../docs/polaris-registration/progress.md)。
