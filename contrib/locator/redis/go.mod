module github.com/byteweap/meta/contrib/locator/redis

go 1.27.1

require (
	github.com/alicebob/miniredis/v2 v2.39.0
	github.com/byteweap/meta v0.0.1
	github.com/redis/go-redis/v9 v9.17.2
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
)

replace github.com/byteweap/meta => ../../..
