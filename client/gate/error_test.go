package gate

import (
	"context"
	"errors"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/envelope"
)

func TestDialReturnsHandshakeRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not authorized", http.StatusUnauthorized)
	}))
	defer server.Close()
	client, err := Dial(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.Nil(t, client)
	require.ErrorIs(t, err, websocket.ErrBadHandshake)
}

func TestRequestRejectsInvalidInputsAndExhaustedSequence(t *testing.T) {
	client, _ := newTestClient(t, WithConnectionSequence())
	_, err := client.Request(t.Context(), "", 1, 1, nil)
	require.ErrorContains(t, err, "service is empty")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = client.Request(ctx, "game", 1, 1, nil)
	require.ErrorIs(t, err, context.Canceled)

	_, err = client.Request(t.Context(), string([]byte{0xff}), 1, 1, nil)
	require.ErrorContains(t, err, "UTF-8")

	client.sequence.Store(math.MaxUint64)
	_, err = client.Request(t.Context(), "game", 1, 1, nil)
	require.ErrorContains(t, err, "sequence exhausted")
	require.Equal(t, uint64(math.MaxUint64), client.sequence.Load())
}

func TestRequestInterruptedBeforeWriterReceives(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "context canceled", true: "closed"}[closed], func(t *testing.T) {
			client := &Client{
				sequence: new(atomic.Uint64), done: make(chan struct{}),
				outgoing: make(chan outgoingRequest),
				pending:  make(map[uint64]chan *envelope.ServerEnvelope),
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := requestInBackground(ctx, client, 1)
			require.Eventually(t, func() bool {
				client.mu.Lock()
				defer client.mu.Unlock()
				return len(client.pending) == 1
			}, time.Second, time.Millisecond)

			want := context.Canceled
			if closed {
				want = errors.New("client closed while waiting to write")
				client.mu.Lock()
				client.err = want
				close(client.done)
				client.mu.Unlock()
			} else {
				cancel()
			}
			require.ErrorIs(t, awaitClientTest(t, result).err, want)
			client.mu.Lock()
			pending := len(client.pending)
			client.mu.Unlock()
			require.Zero(t, pending)
		})
	}
}

func TestInvalidDownstreamMessagesCloseClient(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  []byte
		want string
	}{
		{"invalid protobuf", []byte{0xff}, "decode gate response"},
		{"missing header", nil, "gate response header is missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, peer := newTestClient(t)
			require.NoError(t, peer.WriteMessage(websocket.BinaryMessage, test.raw))
			require.Nil(t, awaitClientTest(t, client.Pushes()))
			_, err := client.Request(t.Context(), "game", 1, 1, nil)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestReadLoopDiscardsNonBinaryAndPushOverflow(t *testing.T) {
	client, peer := newTestClient(t)
	result := requestInBackground(t.Context(), client, 1)
	request := readClientRequest(t, peer)
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, []byte("not protobuf")))
	for i := range pushQueueSize + 1 {
		sendClientMessage(t, peer, &envelope.ServerEnvelope{
			Header: &envelope.Header{Cmd: uint32(i)}, Kind: envelope.MsgType_MSG_TYPE_PUSH,
		})
	}
	sendClientMessage(t, peer, &envelope.ServerEnvelope{Header: request.Header, Kind: envelope.MsgType_MSG_TYPE_RESPONSE})
	require.NoError(t, awaitClientTest(t, result).err)
	require.Len(t, client.Pushes(), pushQueueSize)
	for i := range pushQueueSize {
		require.Equal(t, uint32(i), awaitClientTest(t, client.Pushes()).GetHeader().GetCmd())
	}
}

func TestWriteLoopSkipsCanceledRequestAndClosesOnWriteFailure(t *testing.T) {
	_, peer := newTestClient(t)
	require.NoError(t, peer.Close())
	client := &Client{conn: peer, done: make(chan struct{}), outgoing: make(chan outgoingRequest, 2)}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	client.outgoing <- outgoingRequest{ctx: ctx}
	client.outgoing <- outgoingRequest{ctx: t.Context(), data: []byte("closed")}
	writerExited := make(chan struct{})
	go func() {
		client.writeLoop()
		close(writerExited)
	}()
	awaitClientTest(t, writerExited)
	require.Empty(t, client.outgoing)
	require.Error(t, client.err)
}

type writeDeadlineRecorder struct {
	net.Conn
	deadlines chan<- time.Time
}

func (c writeDeadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	c.deadlines <- deadline
	return c.Conn.SetWriteDeadline(deadline)
}

func TestWriteLoopAppliesCallerDeadline(t *testing.T) {
	url, accepted := startTestGateway(t)
	deadlines := make(chan time.Time, 1)
	dialer := websocket.Dialer{NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := new(net.Dialer).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return writeDeadlineRecorder{Conn: conn, deadlines: deadlines}, nil
	}}
	conn, response, err := dialer.DialContext(t.Context(), url, nil)
	if response != nil {
		require.NoError(t, response.Body.Close())
	}
	require.NoError(t, err)
	peer := awaitClientTest(t, accepted)
	t.Cleanup(func() { _ = peer.Close() })
	client := &Client{conn: conn, done: make(chan struct{}), outgoing: make(chan outgoingRequest, 1)}
	writerExited := make(chan struct{})
	go func() {
		client.writeLoop()
		close(writerExited)
	}()
	t.Cleanup(func() {
		_ = client.Close()
		awaitClientTest(t, writerExited)
	})

	deadline := time.Now().Add(writeTimeout / 2)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	client.outgoing <- outgoingRequest{ctx: ctx, data: []byte("payload")}
	require.True(t, deadline.Equal(awaitClientTest(t, deadlines)))
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	kind, data, err := peer.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, kind)
	require.Equal(t, []byte("payload"), data)
}
