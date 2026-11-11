# Consul Registry

本包实现 `component/registry` 的 Consul 适配器。

**特性**
- 支持健康检查、心跳与服务标签
- 内置本地缓存与 Watch 机制
- 支持自定义 endpoint 解析与检查配置

**安装**

```bash
go get github.com/byteweap/meta/contrib/registry/consul
```

**最小用法**

```go
package main

import (
  "context"

  "github.com/byteweap/meta/component/registry"
  creg "github.com/byteweap/meta/contrib/registry/consul"
  "github.com/hashicorp/consul/api"
)

func main() {
  cli, _ := api.NewClient(api.DefaultConfig())
  reg := creg.New(cli, creg.WithHealthCheck(true))

  svc := &registry.ServiceInstance{
    ID:        "node-1",
    Name:      "game",
    Version:   "v1",
    Endpoints: []string{"grpc://127.0.0.1:9000"},
    Metadata:  map[string]string{"zone": "sh"},
  }

  _ = reg.Register(context.Background(), svc)
  _ = reg.Deregister(context.Background(), svc)
}
```

**配置**
- `WithHealthCheck` 启用健康检查
- `WithTimeout` 设置获取服务超时
- `WithDatacenter` 设置数据中心
- `WithHeartbeat` 启用心跳
- `WithServiceResolver` 自定义 endpoint 解析
- `WithHealthCheckInterval` 设置检查间隔
- `WithDeregisterCriticalServiceAfter` 设置自动注销时间
- `WithServiceCheck` 追加健康检查
- `WithTags` 设置服务标签

**注意事项**
- `GetService` 优先读取已初始化的本地缓存，包括空列表；尚无快照时回源拉取。成功空结果返回 nil 错误，远端请求失败保留原始错误。
- `Watch` 首次查询成功即通知当前快照；新增监听者也会立即收到已有空快照。后续索引变化时广播最新状态，最后一个节点下线会清除旧节点。
- 自定义解析器返回 nil 切片同样表示有效空快照；`ListServices` 包含已缓存的空服务，忽略尚未初始化的服务。
- 后台请求失败时保留上一次成功快照并继续重试；通知可合并，不保证逐个历史状态均被消费。

**验证**

默认测试使用本地 HTTP 替身，不需要运行 Consul：

```bash
go test -mod=readonly ./...
go test -mod=readonly -race ./...
```

依赖真实 Consul 的测试需显式启用 `integration` 标签，并提供本地 Consul 测试环境：

```bash
go test -mod=readonly -tags=integration ./...
```

