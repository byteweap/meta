# NATS Broker (Core)

本包实现 `component/broker` 的 **NATS Core** 版本，目标是“薄、快、稳定”：

- 只封装 `Publish/Subscribe(Queue)/Request` 等通信原语
- 不内置业务级：限流/分级/合并/丢弃/房间 Actor 等（这些应在上层服务实现）

`Sub` 成功返回前会确认服务端已处理订阅，其他连接可立即发布消息。确认过程受 `ConnectTimeout`（默认 2 秒）及调用方上下文的较短期限约束；确认失败会退订并返回错误。订阅存续期由调用方上下文控制，不受这次确认使用的短期上下文影响。

## 最小用法

```go
import (
  "context"
  "time"
  nb "github.com/byteweap/meta/contrib/broker/nats"
  "github.com/byteweap/meta/component/broker"
)

func example() error {

  b, err := nb.New(nb.URLs("nats://127.0.0.1:4222"))
  if err != nil { return err }
  defer b.Close()

  // Queue Group：水平扩展的竞争消费者
  sub, err := b.Sub(context.Background(),
    "wk.game.cmd.route.001.v1",
    func(msg *broker.Message) {
      // 处理 msg.Data
      // 若是 request-reply，可向 msg.Reply 发布响应
    },
    broker.SubQueue("game-route-001"),
  )
  if err != nil { return err }
  defer sub.Unsub()

  // request-reply
  ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
  defer cancel()
  resp, err := b.Request(ctx, "wk.player.qry.get.v1", []byte("hello"))
  _ = resp
  return err
}
```


