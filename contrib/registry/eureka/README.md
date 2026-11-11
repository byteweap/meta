# Eureka Registry

本包实现 `component/registry` 的 Eureka 适配器。

**特性**
- 支持心跳与服务列表刷新
- endpoints 映射为 Eureka Instance
- 同服务可创建多个独立 Watch，通知不阻塞刷新
- 心跳按服务与实例分别管理，注册和注销协调旧任务退出

**安装**

```bash
go get github.com/byteweap/meta/contrib/registry/eureka
```

**最小用法**

```go
package main

import (
  "context"

  "github.com/byteweap/meta/component/registry"
  ereg "github.com/byteweap/meta/contrib/registry/eureka"
)

func main() {
  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()
  reg, err := ereg.New([]string{"http://127.0.0.1:8761"}, ereg.WithContext(ctx))
  if err != nil {
    panic(err)
  }

  svc := &registry.ServiceInstance{
    ID:        "node-1",
    Name:      "game",
    Version:   "v1",
    Endpoints: []string{"http://127.0.0.1:9000"},
  }

  _ = reg.Register(context.Background(), svc)
  _ = reg.Deregister(context.Background(), svc)
}
```

**配置**
- `WithContext` 设置注册中心上下文
- `WithHeartbeat` 设置心跳间隔
- `WithRefresh` 设置刷新间隔
- `WithEurekaPath` 设置 Eureka 接口路径

**注意事项**
- `Endpoints` 将被解析为 Eureka Instance，Metadata 可覆盖 `securePort`、`homePageURL`、`statusPageURL`、`healthCheckURL`
- 构造 Registry 需要至少一个服务器地址，刷新和心跳间隔须大于零。取消 `WithContext` 提供的上下文会停止刷新、取消心跳，并释放监听等待。
- 首次、定时和订阅触发的全量刷新由同一循环处理；成功快照先写入缓存再通知。成功空列表清除旧节点；失败保留上次成功结果，尚无成功快照时返回真实错误。
- `GetService` 等待首轮刷新或取消，完整缓存初始化后，缺少某个服务表示正常空列表。服务分组统一为大写，返回的业务身份仍使用注册元数据。
- 每个 Watch 独立订阅，`Stop` 可重复调用且只取消自身。通知可合并，慢消费者不会阻塞刷新；`Next` 返回最新快照，不保证逐条历史变化都被消费。
- 注册时分别复制每个 endpoint 的 Metadata；返回快照的元数据也独立于缓存和其他结果。
- HTTP 请求使用独立的服务器顺序和可重放请求内容，不修改调用方的地址列表；请求携带 context，默认 HTTP 超时为 3 秒。
- 每个实例的注册与注销串行协调。注销先取消并等待旧心跳及其重注册退出，再发送删除请求；同服务的其他实例不受影响。网络取消不能保证远端尚未处理已经收到的请求。
- Registry 上下文取消后拒绝新注册，但仍可用独立的清理 context 调用 `Deregister`。操作返回错误时应由调用方处理或重试，已停止的旧心跳不会自动恢复。
- 发现查询方法现在返回错误，原回调订阅和手动心跳接口由内部管理替代；不提供旧签名兼容。

**验证**

默认测试使用本地 HTTP/可控传输与虚拟时间，不需要真实 Eureka：

```bash
go test -mod=readonly ./...
go test -mod=readonly -race ./...
```

真实服务测试需显式启用 `integration` 标签，并提供测试中配置的 Eureka 服务：

```bash
go test -mod=readonly -tags=integration ./...
```

