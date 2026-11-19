package meta

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/server"
)

type lifecycleServer struct {
	ready     chan struct{}
	entered   chan context.Context
	start     func(context.Context) error
	stop      func(context.Context) error
	endpoint  func(context.Context) (*url.URL, error)
	stopCalls atomic.Int32
}

func newLifecycleServer() *lifecycleServer {
	return &lifecycleServer{ready: make(chan struct{}), entered: make(chan context.Context, 1)}
}

func (*lifecycleServer) Kind() server.Kind        { return server.KindMesh }
func (s *lifecycleServer) Ready() <-chan struct{} { return s.ready }
func (s *lifecycleServer) Start(ctx context.Context) error {
	s.entered <- ctx
	if s.start != nil {
		return s.start(ctx)
	}
	close(s.ready)
	<-ctx.Done()
	return ctx.Err()
}

func (s *lifecycleServer) Stop(ctx context.Context) error {
	s.stopCalls.Add(1)
	if s.stop != nil {
		return s.stop(ctx)
	}
	return nil
}

func (s *lifecycleServer) Endpoint(ctx context.Context) (*url.URL, error) {
	if s.endpoint != nil {
		return s.endpoint(ctx)
	}
	return &url.URL{Scheme: "mesh", Host: "test:1"}, nil
}

type lifecycleRegistry struct {
	registry.Registry
	register        func(context.Context) error
	deregister      func(context.Context) error
	registerCalls   atomic.Int32
	deregisterCalls atomic.Int32
}

func (r *lifecycleRegistry) Register(ctx context.Context, _ *registry.ServiceInstance) error {
	r.registerCalls.Add(1)
	if r.register != nil {
		return r.register(ctx)
	}
	return nil
}

func (r *lifecycleRegistry) Deregister(ctx context.Context, _ *registry.ServiceInstance) error {
	r.deregisterCalls.Add(1)
	if r.deregister != nil {
		return r.deregister(ctx)
	}
	return nil
}

func awaitLifecycle[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for lifecycle operation")
		var zero T
		return zero
	}
}

func runLifecycleApp(t *testing.T, app *App) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		result <- app.Run()
	}()
	t.Cleanup(func() {
		_ = app.Stop()
		awaitLifecycle(t, finished)
	})
	return result
}

func TestAppWaitsForAllServersBeforeRegistering(t *testing.T) {
	first, second := newLifecycleServer(), newLifecycleServer()
	second.start = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	registered, postStarted := make(chan struct{}), make(chan struct{})
	reg := &lifecycleRegistry{register: func(context.Context) error {
		for _, srv := range []*lifecycleServer{first, second} {
			select {
			case <-srv.Ready():
			default:
				return errors.New("registered before all servers were ready")
			}
		}
		close(registered)
		return nil
	}}
	app := New(Server(first, second), Registry(reg), PostStart(func(context.Context) error {
		close(postStarted)
		return nil
	}))
	result := runLifecycleApp(t, app)
	awaitLifecycle(t, first.entered)
	awaitLifecycle(t, second.entered)
	select {
	case <-registered:
		t.Fatal("registered while a server was not ready")
	case <-postStarted:
		t.Fatal("post-start ran while a server was not ready")
	case <-time.After(20 * time.Millisecond):
	}
	require.Zero(t, reg.registerCalls.Load())
	close(second.ready)
	awaitLifecycle(t, registered)
	awaitLifecycle(t, postStarted)
	require.NoError(t, app.Stop())
	require.NoError(t, awaitLifecycle(t, result))
	require.Equal(t, int32(1), reg.deregisterCalls.Load())
}

func TestAppStartupFailureInterruptsOtherReadinessWaits(t *testing.T) {
	for _, earlyReturn := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "unexpected nil"}[earlyReturn], func(t *testing.T) {
			waiting, failed := newLifecycleServer(), newLifecycleServer()
			waiting.start = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
			startErr := errors.New("start failed")
			failed.start = func(context.Context) error {
				if earlyReturn {
					return nil
				}
				return startErr
			}
			reg := &lifecycleRegistry{}
			app := New(Server(waiting, failed), Registry(reg))
			err := awaitLifecycle(t, runLifecycleApp(t, app))
			if earlyReturn {
				require.Error(t, err)
			} else {
				require.ErrorIs(t, err, startErr)
			}
			require.Zero(t, reg.registerCalls.Load())
			require.Equal(t, int32(1), waiting.stopCalls.Load())
			require.Equal(t, int32(1), failed.stopCalls.Load())
		})
	}
}

func TestAppCleansUpEveryStartupFailure(t *testing.T) {
	for _, stage := range []string{"endpoint", "pre-start", "register", "post-start"} {
		t.Run(stage, func(t *testing.T) {
			first, second := newLifecycleServer(), newLifecycleServer()
			failure := errors.New(stage + " failed")
			reg := &lifecycleRegistry{}
			var postStops atomic.Int32
			opts := []Option{Server(first, second), Registry(reg), PostStop(func(context.Context) error {
				postStops.Add(1)
				return nil
			})}
			switch stage {
			case "endpoint":
				second.endpoint = func(context.Context) (*url.URL, error) { return nil, failure }
			case "pre-start":
				opts = append(opts, PreStart(func(context.Context) error { return failure }))
			case "register":
				reg.register = func(context.Context) error { return failure }
			case "post-start":
				opts = append(opts, PostStart(func(context.Context) error { return failure }))
			}
			err := awaitLifecycle(t, runLifecycleApp(t, New(opts...)))
			require.ErrorIs(t, err, failure)
			require.Equal(t, int32(1), first.stopCalls.Load())
			require.Equal(t, int32(1), second.stopCalls.Load())
			require.Equal(t, int32(1), postStops.Load())
			if stage == "register" || stage == "post-start" {
				require.Equal(t, int32(1), reg.registerCalls.Load())
			} else {
				require.Zero(t, reg.registerCalls.Load())
			}
			if stage == "post-start" {
				require.Equal(t, int32(1), reg.deregisterCalls.Load())
			} else {
				require.Zero(t, reg.deregisterCalls.Load())
			}
		})
	}
}

func TestAppRegistrationResultDeterminesCleanupAfterCancellation(t *testing.T) {
	for _, succeeded := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed", true: "succeeded"}[succeeded], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reg := &lifecycleRegistry{register: func(context.Context) error {
				cancel()
				if !succeeded {
					return context.Canceled
				}
				return nil
			}}
			srv := newLifecycleServer()
			app := New(Context(ctx), Server(srv), Registry(reg), Signal())
			require.NoError(t, awaitLifecycle(t, runLifecycleApp(t, app)))
			require.Equal(t, int32(1), reg.registerCalls.Load())
			require.Equal(t, int32(1), srv.stopCalls.Load())
			if succeeded {
				require.Equal(t, int32(1), reg.deregisterCalls.Load())
			} else {
				require.Zero(t, reg.deregisterCalls.Load())
			}
		})
	}
}

func TestAppPreservesCleanupErrorsAndRunsEveryHook(t *testing.T) {
	failures := []error{
		errors.New("post-start"), errors.New("pre-stop 1"), errors.New("pre-stop 2"),
		errors.New("deregister"), errors.New("stop"), errors.New("post-stop 1"), errors.New("post-stop 2"),
	}
	srv := newLifecycleServer()
	srv.stop = func(context.Context) error { return failures[4] }
	reg := &lifecycleRegistry{deregister: func(context.Context) error { return failures[3] }}
	var app *App
	var preStops, postStops atomic.Int32
	app = New(Server(srv), Registry(reg),
		PostStart(func(context.Context) error { return failures[0] }),
		PreStop(func(context.Context) error {
			preStops.Add(1)
			_ = app.Stop() // Requesting stop from a hook must not deadlock cleanup.
			return failures[1]
		}),
		PreStop(func(context.Context) error { preStops.Add(1); return failures[2] }),
		PostStop(func(context.Context) error { postStops.Add(1); return failures[5] }),
		PostStop(func(context.Context) error { postStops.Add(1); return failures[6] }),
	)
	err := awaitLifecycle(t, runLifecycleApp(t, app))
	for _, failure := range failures {
		require.ErrorIs(t, err, failure)
	}
	require.Equal(t, int32(2), preStops.Load())
	require.Equal(t, int32(2), postStops.Load())
	require.Equal(t, int32(1), reg.deregisterCalls.Load())
	require.Equal(t, int32(1), srv.stopCalls.Load())
}

func TestAppCleanupUsesIndependentContext(t *testing.T) {
	type contextKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "value"))
	defer cancel()
	type snapshot struct {
		err      error
		deadline bool
		info     AppInfo
		value    any
	}
	observed := make(chan snapshot, 1)
	srv := newLifecycleServer()
	srv.stop = func(ctx context.Context) error {
		_, deadline := ctx.Deadline()
		info, _ := FromContext(ctx)
		observed <- snapshot{ctx.Err(), deadline, info, ctx.Value(contextKey{})}
		return nil
	}
	app := New(Context(parent), ID("cleanup-app"), Server(srv), PostStart(func(context.Context) error {
		cancel()
		return nil
	}))
	require.NoError(t, awaitLifecycle(t, runLifecycleApp(t, app)))
	state := awaitLifecycle(t, observed)
	require.NoError(t, state.err)
	require.True(t, state.deadline)
	require.Same(t, app, state.info)
	require.Equal(t, "value", state.value)
	runCtx := awaitLifecycle(t, srv.entered)
	require.ErrorIs(t, runCtx.Err(), context.Canceled)
}

func TestAppConcurrentStopCleansUpOnce(t *testing.T) {
	srv := newLifecycleServer()
	reg := &lifecycleRegistry{}
	started := make(chan struct{})
	var preStops, postStops atomic.Int32
	app := New(Server(srv), Registry(reg),
		PostStart(func(context.Context) error { close(started); return nil }),
		PreStop(func(context.Context) error { preStops.Add(1); return nil }),
		PostStop(func(context.Context) error { postStops.Add(1); return nil }),
	)
	result := runLifecycleApp(t, app)
	awaitLifecycle(t, started)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { _ = app.Stop() })
	}
	wg.Wait()
	require.NoError(t, awaitLifecycle(t, result))
	require.NoError(t, app.Stop())
	require.Equal(t, int32(1), preStops.Load())
	require.Equal(t, int32(1), postStops.Load())
	require.Equal(t, int32(1), reg.deregisterCalls.Load())
	require.Equal(t, int32(1), srv.stopCalls.Load())
	require.Error(t, app.Run())
	require.Equal(t, int32(1), srv.stopCalls.Load())
}

func TestAppStopBeforeReadinessDoesNotRegister(t *testing.T) {
	srv := newLifecycleServer()
	srv.start = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	reg := &lifecycleRegistry{}
	app := New(Server(srv), Registry(reg))
	result := runLifecycleApp(t, app)
	awaitLifecycle(t, srv.entered)
	require.NoError(t, app.Stop())
	require.NoError(t, awaitLifecycle(t, result))
	require.Zero(t, reg.registerCalls.Load())
	require.Equal(t, int32(1), srv.stopCalls.Load())
}

type drainLifecycleServer struct {
	*lifecycleServer
	drains atomic.Int32
}

func (s *drainLifecycleServer) BeginDrain(context.Context) { s.drains.Add(1) }

func TestAppStopStillCancelsDrainerBeforeReady(t *testing.T) {
	srv := &drainLifecycleServer{lifecycleServer: newLifecycleServer()}
	srv.start = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	reg := &lifecycleRegistry{}
	app := New(Server(srv), Registry(reg), Signal())
	done := runLifecycleApp(t, app)
	ctx := awaitLifecycle(t, srv.entered)
	require.NoError(t, app.Stop())
	require.NoError(t, awaitLifecycle(t, done))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.EqualValues(t, 1, srv.drains.Load())
	require.Zero(t, reg.registerCalls.Load())
}

func TestAppCleanupTimeoutStillRunsPostStop(t *testing.T) {
	for _, stage := range []string{"stop request", "startup error"} {
		t.Run(stage, func(t *testing.T) {
			srv := newLifecycleServer()
			startErr := errors.New("start failed")
			if stage == "startup error" {
				srv.start = func(context.Context) error { return startErr }
			}
			srv.stop = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
			var postStops atomic.Int32
			var app *App
			app = New(Server(srv), StopTimeout(20*time.Millisecond),
				PostStart(func(context.Context) error { return app.Stop() }),
				PostStop(func(context.Context) error { postStops.Add(1); return nil }),
			)
			err := awaitLifecycle(t, runLifecycleApp(t, app))
			require.ErrorIs(t, err, context.DeadlineExceeded)
			if stage == "startup error" {
				require.ErrorIs(t, err, startErr)
			}
			require.Equal(t, int32(1), postStops.Load())
			require.Equal(t, int32(1), srv.stopCalls.Load())
		})
	}
}
