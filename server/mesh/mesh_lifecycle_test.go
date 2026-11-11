package mesh

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/locator"
)

type lifecycleBroker struct {
	mockBroker
	subscribe func(context.Context) error
}

func (b *lifecycleBroker) Sub(ctx context.Context, _ string, _ broker.Handler, _ ...broker.SubscribeOption) (broker.Subscription, error) {
	if err := b.subscribe(ctx); err != nil {
		return nil, err
	}
	return &mockSubscription{}, nil
}

// 生命周期测试不发送业务消息，因此不应调用定位器。
type lifecycleLocator struct{ locator.Locator }

type lifecycleAppInfo struct{ meta.AppInfo }

func (lifecycleAppInfo) ID() string   { return "game-1" }
func (lifecycleAppInfo) Name() string { return "game" }

func startLifecycleMesh(t *testing.T, m *Mesh) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(meta.NewContext(ctx, lifecycleAppInfo{})) }()
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		_ = m.Stop(stopCtx)
	})
	return done
}

func TestMeshLifecycleReadyWaitsForSubscription(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	m := New(Locator(lifecycleLocator{}), Broker(&lifecycleBroker{subscribe: func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}))
	ready := m.Ready()
	done := startLifecycleMesh(t, m)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("subscription was not started")
	}
	select {
	case <-ready:
		t.Fatal("mesh became ready before subscription succeeded")
	default:
	}
	close(release)
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("mesh did not become ready")
	}
	require.Equal(t, ready, m.Ready())
	require.NoError(t, m.Stop(context.Background()))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("mesh did not stop")
	}
}

func TestMeshLifecycleSubscriptionFailureDoesNotSignalReady(t *testing.T) {
	want := errors.New("subscription failed")
	m := New(Locator(lifecycleLocator{}), Broker(&lifecycleBroker{subscribe: func(context.Context) error { return want }}))
	done := startLifecycleMesh(t, m)
	select {
	case err := <-done:
		require.ErrorIs(t, err, want)
	case <-time.After(time.Second):
		t.Fatal("mesh did not return its startup failure")
	}
	select {
	case <-m.Ready():
		t.Fatal("failed mesh must not signal readiness")
	default:
	}
}

func TestMeshLifecycleStopBeforeStart(t *testing.T) {
	m := New(Locator(lifecycleLocator{}), Broker(&mockBroker{}))
	require.NoError(t, m.Stop(context.Background()))
	require.ErrorIs(t, m.Start(meta.NewContext(context.Background(), lifecycleAppInfo{})), context.Canceled)
}

func TestMeshLifecycleStopDuringSubscription(t *testing.T) {
	entered := make(chan struct{})
	m := New(Locator(lifecycleLocator{}), Broker(&lifecycleBroker{subscribe: func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}))
	done := startLifecycleMesh(t, m)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("subscription was not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, m.Stop(ctx))
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("startup did not observe stop")
	}
	select {
	case <-m.Ready():
		t.Fatal("stopped mesh must not signal readiness")
	default:
	}
}
