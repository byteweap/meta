package mesh

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

type clientOverloadBroker struct {
	panicBroker
	publish func(context.Context, *broker.Message) error
}

func (b *clientOverloadBroker) Pub(ctx context.Context, subject string, data []byte, opts ...broker.PublishOption) error {
	var options broker.PublishOptions
	for _, opt := range opts {
		opt(&options)
	}
	return b.publish(ctx, &broker.Message{Subject: subject, Header: options.Header, Data: data})
}

func clientOverloadRequest(t *testing.T, seq uint64) *broker.Message {
	t.Helper()
	data, err := proto.Marshal(&envelope.ClientEnvelope{
		Header:  &envelope.Header{Seq: seq, Cmd: 7, Version: 2},
		Service: "game",
	})
	require.NoError(t, err)
	header := cluster.BuildHeader(42, cluster.EventBusiness, "meta.game.gate.gate-1", "gate", "game")
	header.Set(cluster.FieldNameSessionID, "original-session")
	return &broker.Message{Header: header, Data: data}
}

func startFullClientQueue(t *testing.T, b *clientOverloadBroker, opts ...Option) (*Mesh, func(), <-chan uint64) {
	t.Helper()
	m := New(append([]Option{Broker(b), Locator(lifecycleLocator{}), MessageBufferSize(2)}, opts...)...)
	entered, hold := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	executed := make(chan uint64, 4)
	m.Route(7, 2, func(_ *Mesh, _ *broker.Message, frame *envelope.ClientEnvelope) {
		seq := frame.GetHeader().GetSeq()
		if seq == 1 {
			close(entered)
			<-hold
		}
		executed <- seq
	})
	runTestMesh(t, m)
	t.Cleanup(release)
	b.handler(clientOverloadRequest(t, 1))
	<-entered
	b.handler(clientOverloadRequest(t, 2))
	b.handler(clientOverloadRequest(t, 3))
	return m, release, executed
}

func TestClientOverloadRejectsAndPreservesAcceptedOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		published := make(chan *broker.Message, 5)
		bro := &clientOverloadBroker{publish: func(_ context.Context, msg *broker.Message) error {
			published <- msg
			return nil
		}}
		obs := &requestObservation{}
		m, release, executed := startFullClientQueue(t, bro, ClientOverloadRejection(true), Observer(obs))

		for _, seq := range []uint64{4, 0} {
			bro.handler(clientOverloadRequest(t, seq))
			require.Len(t, published, 1)
			msg := <-published
			require.Equal(t, "meta.game.gate.gate-1", msg.Subject)
			require.EqualValues(t, 42, cluster.GetUIDBy(msg.Header))
			require.Equal(t, "original-session", cluster.GetSessionIDBy(msg.Header))
			require.Equal(t, cluster.DeliveryResponse, msg.Header.Get(cluster.FieldNameDelivery))
			var response envelope.ServerEnvelope
			require.NoError(t, proto.Unmarshal(msg.Data, &response))
			require.Equal(t, envelope.MsgType_RESPONSE, response.GetKind())
			require.Equal(t, "game", response.GetService())
			require.Equal(t, seq, response.GetHeader().GetSeq())
			require.EqualValues(t, 7, response.GetHeader().GetCmd())
			require.EqualValues(t, 2, response.GetHeader().GetVersion())
			require.EqualValues(t, http.StatusServiceUnavailable, response.GetError().GetCode())
			require.Empty(t, response.GetPayload())
		}
		unidentified := clientOverloadRequest(t, 5)
		delete(unidentified.Header, cluster.FieldNameSessionID)
		bro.handler(unidentified)
		malformed := clientOverloadRequest(t, 6)
		malformed.Data = []byte{0xff}
		bro.handler(malformed)
		require.Empty(t, published)
		require.Empty(t, executed)

		m.BeginDrain(t.Context())
		bro.handler(clientOverloadRequest(t, 7))
		release()
		require.NoError(t, m.Stop(t.Context()))
		require.Len(t, executed, 3)
		require.Equal(t, []uint64{1, 2, 3}, []uint64{<-executed, <-executed, <-executed})
		require.Empty(t, published)
		require.Equal(t, []observe.Reason{
			observe.QueueFull, observe.QueueFull, observe.QueueFull, observe.QueueFull, observe.Stopping,
		}, obs.reasons)
		require.Len(t, obs.timings[observe.Queue], 3)
		require.Len(t, obs.timings[observe.Reply], 2)
	})
}

func TestClientOverloadPreservesBlockingAdmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		message func(*testing.T) *broker.Message
	}{
		{"disabled", false, func(t *testing.T) *broker.Message { return clientOverloadRequest(t, 4) }},
		{"serial RPC", true, func(*testing.T) *broker.Message { return poolRequest("rpc") }},
		{"session event", true, func(*testing.T) *broker.Message {
			return &broker.Message{Header: sessionEventHeader(42, cluster.EventOnline, "original-session")}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bro := &clientOverloadBroker{}
				m, release, _ := startFullClientQueue(t, bro, ClientOverloadRejection(tc.enabled))
				returned := make(chan struct{})
				msg := tc.message(t)
				go func() {
					bro.handler(msg)
					close(returned)
				}()
				synctest.Wait()
				select {
				case <-returned:
					t.Fatal("message returned while serial queue was full")
				default:
				}
				m.BeginDrain(t.Context())
				<-returned
				release()
				require.NoError(t, m.Stop(t.Context()))
			})
		})
	}
}

func TestClientOverloadBoundsConcurrentReplies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const burst = 32
		publishing, returned := make(chan struct{}, burst+1), make(chan struct{}, burst)
		hold := make(chan struct{})
		finish := sync.OnceFunc(func() { close(hold) })
		defer finish()
		bro := &clientOverloadBroker{publish: func(_ context.Context, _ *broker.Message) error {
			publishing <- struct{}{}
			<-hold
			return nil
		}}
		obs := &requestObservation{}
		m, release, _ := startFullClientQueue(t, bro, ClientOverloadRejection(true), Observer(obs))
		msg := clientOverloadRequest(t, 4)
		var deliveries sync.WaitGroup
		for range burst {
			deliveries.Go(func() {
				bro.handler(msg)
				returned <- struct{}{}
			})
		}
		synctest.Wait()
		require.Len(t, publishing, 1)
		require.Len(t, returned, burst-1)

		finish()
		deliveries.Wait()
		bro.handler(msg)
		require.Len(t, publishing, 2, "回包结束后应能重新获得名额")
		m.BeginDrain(t.Context())
		release()
		require.NoError(t, m.Stop(t.Context()))
		require.Len(t, obs.reasons, burst+1)
		for _, reason := range obs.reasons {
			require.Equal(t, observe.QueueFull, reason)
		}
	})
}

func TestClientOverloadReplyTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := make(chan error, 1)
		bro := &clientOverloadBroker{publish: func(ctx context.Context, _ *broker.Message) error {
			<-ctx.Done()
			result <- ctx.Err()
			return ctx.Err()
		}}
		startFullClientQueue(t, bro, ClientOverloadRejection(true))
		start := time.Now()
		bro.handler(clientOverloadRequest(t, 4))
		require.ErrorIs(t, <-result, context.DeadlineExceeded)
		require.Equal(t, clientOverloadReplyTimeout, time.Since(start))
	})
}

func TestClientOverloadDrainCancelsAndWaitsForReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publishing, hold := make(chan struct{}), make(chan struct{})
		finish := sync.OnceFunc(func() { close(hold) })
		canceled := make(chan error, 1)
		bro := &clientOverloadBroker{publish: func(ctx context.Context, _ *broker.Message) error {
			close(publishing)
			<-ctx.Done()
			canceled <- ctx.Err()
			<-hold
			return ctx.Err()
		}}
		m, release, _ := startFullClientQueue(t, bro, ClientOverloadRejection(true))
		defer finish()
		msg := clientOverloadRequest(t, 4)
		go bro.handler(msg)
		<-publishing
		m.BeginDrain(t.Context())
		require.ErrorIs(t, <-canceled, context.Canceled)
		release()

		stopped := make(chan error, 1)
		go func() { stopped <- m.Stop(t.Context()) }()
		synctest.Wait()
		require.Empty(t, stopped)
		finish()
		require.NoError(t, <-stopped)
	})
}

func TestDecodeClientReplyHeaderMatchesEnvelope(t *testing.T) {
	bytesField := func(n protowire.Number, b []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, n, protowire.BytesType), b)
	}
	encodeHeader := func(h *envelope.Header) []byte {
		data, err := proto.Marshal(h)
		require.NoError(t, err)
		return bytesField(clientEnvelopeHeaderField, data)
	}
	first := encodeHeader(&envelope.Header{Seq: 17, Cmd: 7})
	second := encodeHeader(&envelope.Header{Version: 2})
	zeroSeq := bytesField(clientEnvelopeHeaderField, protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 0))
	payload := bytesField(3, make([]byte, 2*1024))
	invalidField := protowire.AppendVarint(protowire.AppendTag(nil, protowire.MaxValidNumber+1, protowire.VarintType), 0)
	wrongType := protowire.AppendVarint(protowire.AppendTag(nil, clientEnvelopeHeaderField, protowire.VarintType), 1)
	group := protowire.AppendTag(nil, 99, protowire.StartGroupType)
	group = append(group, first...)
	group = protowire.AppendTag(group, 99, protowire.EndGroupType)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty header", bytesField(clientEnvelopeHeaderField, nil)},
		{"payload before header", slices.Concat(payload, first)},
		{"merge headers and explicit zero", slices.Concat(first, second, zeroSeq)},
		{"unknown group and wrong wire type", slices.Concat(group, wrongType, second)},
		{"unknown group is not a header", group},
		{"missing header", payload},
		{"malformed header", bytesField(clientEnvelopeHeaderField, []byte{0xff})},
		{"truncated payload", slices.Concat(first, payload[:len(payload)-1])},
		{"invalid UTF-8 service", slices.Concat(first, bytesField(clientEnvelopeServiceField, []byte{0xff}))},
		{"invalid field number", slices.Concat(first, invalidField)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var frame envelope.ClientEnvelope
			err := proto.Unmarshal(tc.data, &frame)
			var header envelope.Header
			got := decodeClientReplyHeader(tc.data, &header)
			require.Equal(t, err == nil && frame.GetHeader() != nil, got)
			if got {
				want := frame.GetHeader()
				require.Equal(t, want.GetSeq(), header.GetSeq())
				require.Equal(t, want.GetCmd(), header.GetCmd())
				require.Equal(t, want.GetVersion(), header.GetVersion())
			}
		})
	}
}
