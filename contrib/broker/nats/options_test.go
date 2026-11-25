package nats

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/broker"
)

func TestConnectionOptionTranslation(t *testing.T) {
	options := defaultOptions()
	for _, configure := range []Option{
		URLs("nats://one", "nats://two"), Name("worker"),
		ConnectTimeout(time.Second), Reconnect(3*time.Second, 4), Ping(2*time.Second, 5),
		UserPass("user", "pass"),
	} {
		configure(options)
	}
	options.tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	options.natsOptions = []nats.Option{nats.NoEcho()}
	require.Equal(t, "nats://one,nats://two", options.urls)

	for _, token := range []string{"", "token"} {
		Token(token)(options)
		var got nats.Options
		for _, apply := range buildNatsOptions(options) {
			require.NoError(t, apply(&got))
		}
		require.Equal(t, "worker", got.Name)
		require.Equal(t, time.Second, got.Timeout)
		require.Equal(t, 3*time.Second, got.ReconnectWait)
		require.Equal(t, 4, got.MaxReconnect)
		require.Equal(t, 2*time.Second, got.PingInterval)
		require.Equal(t, 5, got.MaxPingsOut)
		require.True(t, got.Secure && got.NoEcho)
		require.Equal(t, options.tlsCfg, got.TLSConfig)
		if token == "" {
			require.Equal(t, "user", got.User)
			require.Equal(t, "pass", got.Password)
		} else {
			require.Equal(t, token, got.Token)
			require.Empty(t, got.User)
		}
	}
}

func TestConnectionErrorsAndSubscriptionLifecycle(t *testing.T) {
	instance, err := New(URLs("nats://[::1"))
	require.Nil(t, instance)
	require.Error(t, err)
	require.NoError(t, NewWith(nil).Close())

	server := runNatsServer(t)
	conn, err := nats.Connect(server.ClientURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	instance = NewWith(conn)
	require.Equal(t, ID, instance.ID())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg, err := instance.Request(ctx, "missing", nil)
	require.Nil(t, msg)
	require.ErrorIs(t, err, nats.ErrNoResponders)
	sub, err := instance.Sub(ctx, "", func(*broker.Message) {})
	require.Nil(t, sub)
	require.Error(t, err)

	received := make(chan *broker.Message, 1)
	sub, err = instance.Sub(ctx, "example", func(msg *broker.Message) { received <- msg })
	require.NoError(t, err)
	require.NoError(t, instance.Pub(ctx, "example", []byte("data"), broker.PubReply("reply.example")))
	select {
	case msg := <-received:
		require.Equal(t, "reply.example", msg.Reply)
		require.Equal(t, []byte("data"), msg.Data)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, sub.Unsub())
	sub, err = instance.Sub(ctx, "graceful", func(*broker.Message) {})
	require.NoError(t, err)
	require.NoError(t, sub.Close())

	conn.Close()
	require.ErrorIs(t, instance.Close(), nats.ErrConnectionClosed)
}

func TestHeaderConversionDoesNotAliasValues(t *testing.T) {
	require.Nil(t, toNatsHeader(nil))
	require.Nil(t, fromNatsHeader(nil))
	original := broker.Header{"empty": nil, "sender": {"gate"}}
	wire := toNatsHeader(original)
	require.NotContains(t, wire, "empty")
	wire["empty"] = nil
	decoded := fromNatsHeader(wire)
	require.NotContains(t, decoded, "empty")
	original["sender"][0] = "changed"
	wire["sender"][0] = "changed again"
	require.Equal(t, []string{"gate"}, decoded["sender"])
}
