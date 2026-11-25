// Package gate 提供基于 WebSocket 的 Meta Gate 客户端
package gate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/envelope"
)

const (
	writeTimeout  = 5 * time.Second
	pushQueueSize = 32
)

var processSequence atomic.Uint64

// Option 配置 Dial 创建的客户端
type Option func(*dialOptions)

type dialOptions struct {
	connectionSequence bool
}

// WithConnectionSequence 允许每条新连接从 1 分配请求序号
// 仅在 Gate 已启用严格会话投递且回包方都已迁移时使用。默认在同一进程内不跨连接复用序号
func WithConnectionSequence() Option {
	return func(options *dialOptions) {
		options.connectionSequence = true
	}
}

type outgoingRequest struct {
	ctx  context.Context
	data []byte
}

// Client 管理单条连接，可并发发起请求。不支持自动重连或重放，断线后应另行 Dial
type Client struct {
	conn     *websocket.Conn
	sequence *atomic.Uint64
	outgoing chan outgoingRequest
	pushes   chan *envelope.ServerEnvelope
	done     chan struct{}

	mu      sync.Mutex
	pending map[uint64]chan *envelope.ServerEnvelope
	err     error
}

// Dial 连接 Gate。ctx 和 header 用于握手，不控制连接建立后的生命周期
func Dial(ctx context.Context, url string, header http.Header, options ...Option) (*Client, error) {
	conn, response, err := websocket.DefaultDialer.DialContext(ctx, url, header)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	client := &Client{
		conn:     conn,
		sequence: &processSequence,
		outgoing: make(chan outgoingRequest),
		pushes:   make(chan *envelope.ServerEnvelope, pushQueueSize),
		done:     make(chan struct{}),
		pending:  make(map[uint64]chan *envelope.ServerEnvelope),
	}
	var config dialOptions
	for _, option := range options {
		option(&config)
	}
	if config.connectionSequence {
		client.sequence = new(atomic.Uint64)
	}
	go client.readLoop()
	go client.writeLoop()
	return client, nil
}

// Request 发送业务 Protobuf 字节并等待对应响应。业务失败由返回包的 Error 表示，Go error 表示请求未取得响应
// ctx 取消只停止本地等待，不撤销已发出的请求。建议为每次请求设置截止时间
// 并发调用时，序号仅用于关联，不承诺序号顺序与发送顺序一致
func (client *Client) Request(
	ctx context.Context, service string, cmd, version uint32, payload []byte,
) (*envelope.ServerEnvelope, error) {
	if service == "" {
		return nil, errors.New("gate request service is empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	seq, err := nextSequence(client.sequence)
	if err != nil {
		return nil, err
	}
	frame, err := proto.Marshal(&envelope.ClientEnvelope{
		Header:  &envelope.Header{Seq: seq, Cmd: cmd, Version: version, SentAtUnixMs: time.Now().UnixMilli()},
		Service: service,
		Payload: payload,
	})
	if err != nil {
		return nil, err
	}
	response := make(chan *envelope.ServerEnvelope, 1)
	client.mu.Lock()
	if terminalErr := client.err; terminalErr != nil {
		client.mu.Unlock()
		return nil, terminalErr
	}
	client.pending[seq] = response
	client.mu.Unlock()
	defer func() {
		client.mu.Lock()
		delete(client.pending, seq)
		client.mu.Unlock()
	}()

	select {
	case client.outgoing <- outgoingRequest{ctx: ctx, data: frame}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-client.done:
		return nil, client.err
	}
	select {
	case reply := <-response:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-client.done:
		select {
		case reply := <-response:
			return reply, nil
		default:
			return nil, client.err
		}
	}
}

// Pushes 返回推送通道。队列满时丢弃新推送，不阻塞响应分发；连接结束后关闭通道
func (client *Client) Pushes() <-chan *envelope.ServerEnvelope { return client.pushes }

// Close 关闭连接并解除所有请求等待。可重复调用
func (client *Client) Close() error { return client.shutdown(net.ErrClosed) }

func nextSequence(counter *atomic.Uint64) (uint64, error) {
	for {
		last := counter.Load()
		if last == math.MaxUint64 {
			return 0, errors.New("gate request sequence exhausted")
		}
		if counter.CompareAndSwap(last, last+1) {
			return last + 1, nil
		}
	}
}

func (client *Client) shutdown(reason error) error {
	client.mu.Lock()
	if client.err != nil {
		client.mu.Unlock()
		return nil
	}
	client.err = reason
	clear(client.pending)
	close(client.done)
	client.mu.Unlock()
	return client.conn.Close()
}

func (client *Client) readLoop() {
	defer close(client.pushes)
	for {
		kind, frame, err := client.conn.ReadMessage()
		if err != nil {
			_ = client.shutdown(err)
			return
		}
		if kind != websocket.BinaryMessage {
			continue
		}
		message := &envelope.ServerEnvelope{}
		if err := proto.Unmarshal(frame, message); err != nil {
			_ = client.shutdown(fmt.Errorf("decode gate response: %w", err))
			return
		}
		if message.GetHeader() == nil {
			_ = client.shutdown(errors.New("gate response header is missing"))
			return
		}
		switch message.GetKind() {
		case envelope.MsgType_MSG_TYPE_RESPONSE:
			seq := message.GetHeader().GetSeq()
			client.mu.Lock()
			if response := client.pending[seq]; response != nil {
				delete(client.pending, seq)
				response <- message
			}
			client.mu.Unlock()
		case envelope.MsgType_MSG_TYPE_PUSH:
			select {
			case client.pushes <- message:
			default:
			}
		}
	}
}

func (client *Client) writeLoop() {
	for {
		select {
		case <-client.done:
			return
		case request := <-client.outgoing:
			if request.ctx.Err() != nil {
				continue
			}
			deadline := time.Now().Add(writeTimeout)
			if limit, ok := request.ctx.Deadline(); ok && limit.Before(deadline) {
				deadline = limit
			}
			// Gorilla 的 SetWriteDeadline 只记录截止时间并返回 nil
			_ = client.conn.SetWriteDeadline(deadline)
			if err := client.conn.WriteMessage(websocket.BinaryMessage, request.data); err != nil {
				_ = client.shutdown(err)
				return
			}
		}
	}
}
