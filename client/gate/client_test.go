package gate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/envelope"
)

func startTestGateway(t *testing.T) (string, <-chan *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 2)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		accepted <- conn
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http"), accepted
}

func newTestClient(t *testing.T, options ...Option) (*Client, *websocket.Conn) {
	t.Helper()
	url, accepted := startTestGateway(t)
	return dialTestClient(t, url, accepted, options...)
}

func dialTestClient(t *testing.T, url string, accepted <-chan *websocket.Conn, options ...Option) (*Client, *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := Dial(ctx, url, nil, options...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	peer := awaitClientTest(t, accepted)
	t.Cleanup(func() { _ = peer.Close() })
	return client, peer
}

func awaitClientTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for client test result")
		var zero T
		return zero
	}
}

func readClientRequest(t *testing.T, peer *websocket.Conn) *envelope.ClientEnvelope {
	t.Helper()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(2*time.Second)))
	kind, data, err := peer.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, kind)
	request := &envelope.ClientEnvelope{}
	require.NoError(t, proto.Unmarshal(data, request))
	return request
}

func sendClientMessage(t *testing.T, peer *websocket.Conn, msg *envelope.ServerEnvelope) {
	t.Helper()
	data, err := proto.Marshal(msg)
	require.NoError(t, err)
	require.NoError(t, peer.SetWriteDeadline(time.Now().Add(2*time.Second)))
	require.NoError(t, peer.WriteMessage(websocket.BinaryMessage, data))
}

type clientTestResult struct {
	reply *envelope.ServerEnvelope
	err   error
}

func requestInBackground(ctx context.Context, client *Client, cmd uint32) <-chan clientTestResult {
	result := make(chan clientTestResult, 1)
	go func() {
		reply, err := client.Request(ctx, "game", cmd, 1, []byte{0x08, 0x01})
		result <- clientTestResult{reply: reply, err: err}
	}()
	return result
}

func TestClientMatchesOutOfOrderResponsesAndPushes(t *testing.T) {
	client, peer := newTestClient(t, WithConnectionSequence())
	first := requestInBackground(context.Background(), client, 10)
	firstRequest := readClientRequest(t, peer)
	second := requestInBackground(context.Background(), client, 20)
	secondRequest := readClientRequest(t, peer)
	require.Equal(t, uint64(1), firstRequest.GetHeader().GetSeq())
	require.Equal(t, uint64(2), secondRequest.GetHeader().GetSeq())
	require.Equal(t, "game", secondRequest.GetService())
	require.Positive(t, secondRequest.GetHeader().GetSentAtUnixMs())
	require.Equal(t, []byte{0x08, 0x01}, secondRequest.GetPayload())

	failure := &envelope.ResponseError{Code: 409, Message: "room is full"}
	sendClientMessage(t, peer, &envelope.ServerEnvelope{
		Header: secondRequest.Header, Kind: envelope.MsgType_RESPONSE, Service: "game", Error: failure,
	})
	secondResult := awaitClientTest(t, second)
	require.NoError(t, secondResult.err)
	require.Equal(t, uint32(20), secondResult.reply.GetHeader().GetCmd())
	require.True(t, proto.Equal(failure, secondResult.reply.GetError()))

	sendClientMessage(t, peer, &envelope.ServerEnvelope{Header: &envelope.Header{Cmd: 30}, Kind: envelope.MsgType_PUSH})
	require.Equal(t, uint32(30), awaitClientTest(t, client.Pushes()).GetHeader().GetCmd())
	sendClientMessage(t, peer, &envelope.ServerEnvelope{Header: firstRequest.Header, Kind: envelope.MsgType_RESPONSE})
	firstResult := awaitClientTest(t, first)
	require.NoError(t, firstResult.err)
	require.Equal(t, uint32(10), firstResult.reply.GetHeader().GetCmd())
}

func TestClientClearsRequestsAfterCancelAndDisconnect(t *testing.T) {
	client, peer := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := requestInBackground(ctx, client, 10)
	firstRequest := readClientRequest(t, peer)
	cancel()
	require.ErrorIs(t, awaitClientTest(t, canceled).err, context.Canceled)
	client.mu.Lock()
	pending := len(client.pending)
	client.mu.Unlock()
	require.Zero(t, pending)

	next := requestInBackground(context.Background(), client, 20)
	secondRequest := readClientRequest(t, peer)
	require.Greater(t, secondRequest.GetHeader().GetSeq(), firstRequest.GetHeader().GetSeq())
	sendClientMessage(t, peer, &envelope.ServerEnvelope{Header: firstRequest.Header, Kind: envelope.MsgType_RESPONSE})
	require.NoError(t, peer.Close())
	require.Error(t, awaitClientTest(t, next).err)
	require.Nil(t, awaitClientTest(t, client.Pushes()))
	client.mu.Lock()
	pending = len(client.pending)
	client.mu.Unlock()
	require.Zero(t, pending)
}

func TestClientTimesOutAwaitingResponse(t *testing.T) {
	client, peer := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result := requestInBackground(ctx, client, 10)
	readClientRequest(t, peer)
	require.ErrorIs(t, awaitClientTest(t, result).err, context.DeadlineExceeded)
}

func TestClientSequenceScopeAcrossConnections(t *testing.T) {
	url, accepted := startTestGateway(t)
	for _, test := range []struct {
		name    string
		options []Option
	}{
		{name: "default"},
		{name: "strict deployment", options: []Option{WithConnectionSequence()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sequences := make([]uint64, 0, 2)
			for range 2 {
				client, peer := dialTestClient(t, url, accepted, test.options...)
				requested := requestInBackground(context.Background(), client, 10)
				request := readClientRequest(t, peer)
				sequences = append(sequences, request.GetHeader().GetSeq())
				sendClientMessage(t, peer, &envelope.ServerEnvelope{Header: request.Header, Kind: envelope.MsgType_RESPONSE})
				require.NoError(t, awaitClientTest(t, requested).err)
				require.NoError(t, client.Close())
			}
			if len(test.options) == 0 {
				require.Greater(t, sequences[1], sequences[0])
			} else {
				require.Equal(t, []uint64{1, 1}, sequences)
			}
		})
	}
}
