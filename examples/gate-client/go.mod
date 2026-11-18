module github.com/byteweap/meta/examples/gate-client

go 1.27.1

require (
	github.com/byteweap/meta v0.0.1
	google.golang.org/protobuf v1.36.11
)

require github.com/gorilla/websocket v1.5.0 // indirect

replace github.com/byteweap/meta => ../..
