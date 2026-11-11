package gate

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/internal/cluster"
)

type shutdownConnectionContext struct {
	err      error
	deadline bool
}

func observeShutdownConnectionContext(ctx context.Context) shutdownConnectionContext {
	_, deadline := ctx.Deadline()
	return shutdownConnectionContext{ctx.Err(), deadline}
}

type shutdownConnectionLocator struct {
	testLocator
	unbinding chan shutdownConnectionContext
	release   <-chan struct{}
	unbindErr error
}

func (l *shutdownConnectionLocator) Release(ctx context.Context, uid int64, service, node, sessionID string) (bool, error) {
	released, _ := l.testLocator.Release(ctx, uid, service, node, sessionID)
	l.unbinding <- observeShutdownConnectionContext(ctx)
	select {
	case <-l.release:
		return released, errors.Join(l.unbindErr, ctx.Err())
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

type shutdownConnectionBroker struct {
	testBroker
	online     chan struct{}
	offline    chan shutdownConnectionContext
	offlineErr error
}

func (b *shutdownConnectionBroker) Pub(ctx context.Context, subject string, data []byte, opts ...broker.PublishOption) error {
	o := &broker.PublishOptions{}
	for _, opt := range opts {
		opt(o)
	}
	_ = b.testBroker.Pub(ctx, subject, data, opts...)
	switch cluster.GetEventBy(o.Header) {
	case cluster.EventOnline:
		b.online <- struct{}{}
	case cluster.EventOffline:
		b.offline <- observeShutdownConnectionContext(ctx)
		return errors.Join(b.offlineErr, ctx.Err())
	}
	return ctx.Err()
}

func awaitShutdownConnection[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connection shutdown")
		var zero T
		return zero
	}
}

func startShutdownConnectionGate(t *testing.T, g *Gate) (string, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		finished <- g.Start(meta.NewContext(ctx, testAppInfo{id: "gate-1", name: "gate"}))
	}()
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		_ = g.Stop(stopCtx)
		require.NoError(t, awaitShutdownConnection(t, finished))
	})
	awaitShutdownConnection(t, g.Ready())
	endpoint, err := g.Endpoint(context.Background())
	require.NoError(t, err)
	return endpoint.String(), cancel
}

func TestGateShutdownWaitsForDisconnectCleanup(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		disconnectBeforeStop bool
		releaseError         bool
		offlineError         bool
	}{
		{name: "cancel before stop"},
		{name: "disconnect before cancellation", disconnectBeforeStop: true},
		{name: "shared release error", releaseError: true},
		{name: "shared publication error", offlineError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			loc := &shutdownConnectionLocator{
				testLocator: testLocator{nodes: map[string]string{"game": "game-1"}},
				unbinding:   make(chan shutdownConnectionContext, 4),
				release:     release,
			}
			bro := &shutdownConnectionBroker{
				online: make(chan struct{}, 1), offline: make(chan shutdownConnectionContext, 4),
			}
			if tc.releaseError {
				loc.unbindErr = errors.New("unbind failed")
			}
			if tc.offlineError {
				bro.offlineErr = errors.New("offline publication failed")
			}
			g := newLifecycleGate(bro)
			g.opts.locator = loc
			var authentications atomic.Int32
			g.opts.userIDExtractor = func(*http.Request) int64 {
				authentications.Add(1)
				return 42
			}
			address, cancelRun := startShutdownConnectionGate(t, g)
			t.Cleanup(unblock)
			conn, _, err := websocket.DefaultDialer.Dial(address, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			awaitShutdownConnection(t, bro.online)

			var unbindState shutdownConnectionContext
			if tc.disconnectBeforeStop {
				require.NoError(t, conn.Close())
				unbindState = awaitShutdownConnection(t, loc.unbinding)
			}
			cancelRun()
			stopCtx, cancelStop := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelStop()
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { first <- g.Stop(stopCtx) }()
			if !tc.disconnectBeforeStop {
				unbindState = awaitShutdownConnection(t, loc.unbinding)
			}
			require.Eventually(t, func() bool {
				g.lifecycleMu.Lock()
				defer g.lifecycleMu.Unlock()
				return g.stopped
			}, time.Second, time.Millisecond)
			require.NoError(t, unbindState.err)
			require.True(t, unbindState.deadline)
			go func() { second <- g.Stop(stopCtx) }()
			select {
			case <-first:
				t.Fatal("first Stop returned before disconnect cleanup finished")
			case <-second:
				t.Fatal("concurrent Stop returned before disconnect cleanup finished")
			case <-time.After(20 * time.Millisecond):
			}

			response := httptest.NewRecorder()
			g.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
			require.Equal(t, int32(1), authentications.Load())
			unblock()
			for _, result := range []error{awaitShutdownConnection(t, first), awaitShutdownConnection(t, second), g.Stop(context.Background())} {
				if tc.releaseError {
					require.ErrorIs(t, result, loc.unbindErr)
				} else if tc.offlineError {
					require.ErrorIs(t, result, bro.offlineErr)
				} else {
					require.NoError(t, result)
				}
			}
			if !tc.releaseError {
				offlineState := awaitShutdownConnection(t, bro.offline)
				require.NoError(t, offlineState.err)
				require.True(t, offlineState.deadline)
			}
			loc.mu.Lock()
			binds, unbinds := loc.bindCalls, loc.unbindCalls
			loc.mu.Unlock()
			require.Equal(t, 1, binds)
			require.Equal(t, 1, unbinds)
			require.Empty(t, bro.offline)
			_, exists := g.sessions.get(42)
			require.False(t, exists)
			require.True(t, g.ws.IsClosed())
		})
	}
}

func TestGateShutdownWaitsForLateHandshake(t *testing.T) {
	authEntered, releaseAuth := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseAuth) })
	loc, bro := &testLocator{}, &testBroker{}
	g := newLifecycleGate(bro)
	g.opts.locator = loc
	var authentications atomic.Int32
	g.opts.userIDExtractor = func(*http.Request) int64 {
		if authentications.Add(1) == 1 {
			close(authEntered)
			<-releaseAuth
		}
		return 42
	}
	address, _ := startShutdownConnectionGate(t, g)
	t.Cleanup(unblock)
	type dialResult struct {
		conn *websocket.Conn
		err  error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		conn, response, err := websocket.DefaultDialer.Dial(address, nil)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		dialed <- dialResult{conn, err}
	}()
	awaitShutdownConnection(t, authEntered)
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelStop()
	stopped := make(chan error, 1)
	go func() { stopped <- g.Stop(stopCtx) }()
	require.Eventually(t, func() bool {
		g.lifecycleMu.Lock()
		defer g.lifecycleMu.Unlock()
		return g.stopped
	}, time.Second, time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("Stop returned before the admitted handshake finished")
	case <-time.After(20 * time.Millisecond):
	}
	response := httptest.NewRecorder()
	g.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, int32(1), authentications.Load())
	unblock()
	result := awaitShutdownConnection(t, dialed)
	if result.conn != nil {
		t.Cleanup(func() { _ = result.conn.Close() })
		require.NoError(t, result.conn.SetReadDeadline(time.Now().Add(time.Second)))
		_, _, err := result.conn.ReadMessage()
		require.Error(t, err, "a late upgraded connection must be closed")
	} else {
		require.Error(t, result.err)
	}
	require.NoError(t, awaitShutdownConnection(t, stopped))
	loc.mu.Lock()
	binds, unbinds := loc.bindCalls, loc.unbindCalls
	loc.mu.Unlock()
	require.Zero(t, binds)
	require.Zero(t, unbinds)
	bro.mu.Lock()
	publications := len(bro.pubSubjects)
	bro.mu.Unlock()
	require.Zero(t, publications)
	require.True(t, g.ws.IsClosed())
	require.Zero(t, g.ws.Len())
}
