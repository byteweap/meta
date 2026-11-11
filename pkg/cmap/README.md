# cmap

`cmap` 是一个并发安全的分片 map，实现方式为“多个分片 + 每分片一把读写锁”。它适用于高并发访问场景，可有效减少单锁竞争。

## 快速使用

`cmap` 使用单一构造函数 `New(hash, shardCount)`，返回 map 和错误。调用时显式指定哈希函数和正数分片数量，可使用 `DefaultShardCount` 常量（32）：

```go
// string 作为 key
m, err := cmap.New[string, *Session](cmap.FNV1a, cmap.DefaultShardCount)
if err != nil {
    panic(err)
}

// 写入
m.Set(playerID, session)

// 读取
sess, ok := m.Get(playerID)
if ok {
    // ...
}

// 删除
m.Remove(playerID)
```

对于实现了 `fmt.Stringer` 的类型：

```go
// K 实现 fmt.Stringer
m, err := cmap.New[MyKey, *Session](cmap.FNV1aStr[MyKey], cmap.DefaultShardCount)
if err != nil {
    panic(err)
}
```

## uint64 用户 ID 的推荐分片

如果 key 是 `uint64` 类型的用户 ID，建议先做 64 位混合（mix）再取低 32 位，以改善分布：

```go
m, err := cmap.New[uint64, *Session](cmap.Mix64, cmap.DefaultShardCount)
if err != nil {
    panic(err)
}
```

## 分片配置与并发边界

分片数量在构造时固定，每个实例独立持有分片。需要其他数量时，直接传入正整数，例如 `cmap.New[string, int](cmap.FNV1a, 3)`；不要求分片数为 2 的幂。非正数量或 nil 哈希函数返回错误。原全局 `ShardCount` 和单参数构造接口已移除。

哈希函数必须返回稳定结果并支持并发调用；使用 map 期间不能改变同一个键的路由结果。应通过 `New` 初始化后使用。

各操作仍使用分片锁。`Count`、`Keys`、`Iter`、`Items`、`Clear` 和 JSON 操作在并发修改时不保证跨分片原子快照。分片隔离修复与验证见 [变更记录](../../docs/cmap-shard-configuration/progress.md)。

## 内置分片算法（hash.go）

- `FNV1a(key string) uint32`：FNV-1a 32 位哈希
- `FNV1aStr[K fmt.Stringer](key K) uint32`：对 `Stringer` 的 `String()` 结果做 FNV-1a

## 常用 API

- `Set(key K, value V)`
- `Get(key K) (V, bool)`
- `SetIfAbsent(key K, value V) bool`
- `Upsert(key K, value V, cb UpsertCb[V]) V`
- `Remove(key K)`
- `RemoveCb(key K, cb RemoveCb[K, V]) bool`
- `Pop(key K) (V, bool)`
- `Has(key K) bool`
- `Count() int`
- `IsEmpty() bool`
- `Keys() []K`
- `Items() map[K]V`
- `Iter() <-chan Tuple[K, V)`
- `IterCb(fn IterCb[K, V])`
- `Clear()`
- `MarshalJSON() ([]byte, error)`
- `UnmarshalJSON(b []byte) error`

## 性能提醒

`sharding` 算法会直接影响分片分布与锁竞争，进而显著影响 `cmap` 性能。请务必结合实际 key 分布与业务并发特征做好基准测试后再选型。

## 适用场景对比：cmap vs sync.Map

### cmap 更适合
- 写入并发高、读写比例接近、写多读少 的场景
- key 分布相对均匀，热点不明显
- 需要更可控的锁竞争模型

### sync.Map 更适合
- 读多写少、key 生命周期较长的场景
- 热点 key 被频繁读取
- 业务以缓存访问为主，写入较少

## 结合长链接网关场景的建议

对于“玩家 ID -> Session”的网关管理场景：
- 玩家上线/下线是低频写入
- 每条消息都需要查找 Session（高频读取）
- Session 生命周期较长（1–30 分钟）

因此整体更偏“读多写少 + key 生命周期长”的特征，通常 `sync.Map` 的读路径更有优势。

如果你需要调整分片数量、统计或全量遍历，或者实际写入比例更高，再考虑使用 `cmap`；具体性能应使用实际负载验证。
