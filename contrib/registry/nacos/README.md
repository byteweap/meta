# Nacos Registry

本包实现 `component/registry` 的 Nacos 适配器。

**特性**
- 支持 group/cluster 维度注册
- 支持权重与默认协议类型
- 查询与 Watch 共用实例筛选和身份转换，支持正常空快照

**安装**

```bash
go get github.com/byteweap/meta/contrib/registry/nacos
```

**最小用法**

```go
package main

import (
  "context"

  "github.com/byteweap/meta/component/registry"
  nreg "github.com/byteweap/meta/contrib/registry/nacos"
  "github.com/nacos-group/nacos-sdk-go/clients"
  "github.com/nacos-group/nacos-sdk-go/common/constant"
  "github.com/nacos-group/nacos-sdk-go/vo"
)

func main() {
  cli, err := clients.NewNamingClient(vo.NacosClientParam{
    ClientConfig: &constant.ClientConfig{UpdateCacheWhenEmpty: true},
    ServerConfigs: []constant.ServerConfig{*constant.NewServerConfig("127.0.0.1", 8848)},
  })
  if err != nil {
    panic(err)
  }
  reg := nreg.New(cli, nreg.WithGroup("DEFAULT_GROUP"))

  svc := &registry.ServiceInstance{
    ID:        "node-1",
    Name:      "game",
    Version:   "v1",
    Endpoints: []string{"grpc://127.0.0.1:9000"},
  }

  _ = reg.Register(context.Background(), svc)
  _ = reg.Deregister(context.Background(), svc)
}
```

**配置**
- `WithPrefix` 设置前缀路径
- `WithWeight` 设置权重
- `WithCluster` 设置集群名
- `WithGroup` 设置分组
- `WithDefaultKind` 设置默认协议类型

**注意事项**
- `ServiceInstance.ID` 和 `Name` 不能为空，否则注册返回错误。
- 注册复制每个 endpoint 的元数据，再写入保留字段 `id`、`version`、`kind`，不会修改调用方的 map。Metadata 中可通过 `weight` 覆盖注册权重。
- 查询和监听使用相同 Group/Cluster，仅返回健康、启用且权重为正有限值的实例；空列表正常返回，不视为错误。
- 创建 SDK 客户端时必须设置 `UpdateCacheWhenEmpty: true`，否则 SDK 默认可能保留最后一个节点下线前的缓存，适配器无法收到对应空快照。`New` 接收已创建的客户端，不能代为修改此配置。
- 返回 ID 使用 Metadata 中的应用 `id`，Name 使用请求的原始服务名，不采用 Nacos 的 `InstanceId` 或带分组的名称。缺失应用 ID 的实例跳过并记录；旧实例须重新注册，不提供历史 ID 回退。
- 返回元数据独立于 SDK 缓存和其他查询结果；权重统一向上取整，协议缺失或为空时使用 `WithDefaultKind` 的配置。
- SDK 查询不接受 context；取消在调用前后检查，不能强制中断正在进行的 SDK 请求。

**验证**

默认测试使用本地客户端替身，不需要运行 Nacos：

```bash
go test -mod=readonly ./...
go test -mod=readonly -race ./...
```

原有集成测试需显式启用 `integration` 标签，并在 `127.0.0.1:8848` 提供 Nacos：

```bash
go test -mod=readonly -tags=integration ./...
```
