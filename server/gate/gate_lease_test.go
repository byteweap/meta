package gate

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
)

type leaseTestLocator struct {
	testLocator
	acquire func(context.Context, string, time.Duration) error
	renew   func(context.Context, string, time.Duration) (bool, error)
}

func (l *leaseTestLocator) AcquireGateLease(ctx context.Context, id string, ttl time.Duration) error {
	return l.acquire(ctx, id, ttl)
}

func (l *leaseTestLocator) RenewGateLease(ctx context.Context, id string, ttl time.Duration) (bool, error) {
	return l.renew(ctx, id, ttl)
}

func TestGateLeaseAcquisitionFailureKeepsGateUnready(t *testing.T) {
	want := errors.New("lease storage unavailable")
	g := newLifecycleGate(&testBroker{})
	g.opts.locator = &leaseTestLocator{acquire: func(context.Context, string, time.Duration) error { return want }}
	done := startLifecycleGate(t, g)
	require.ErrorIs(t, awaitBackground(t, done), want)
	select {
	case <-g.Ready():
		t.Fatal("Gate became ready without its run lease")
	default:
	}
}

type leaseTestListener struct {
	closed chan struct{}
	once   sync.Once
}

func (l *leaseTestListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *leaseTestListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (*leaseTestListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}
}

func TestGateRenewsRunLeaseAndStopsWhenItExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newLifecycleGate(&testBroker{})
		g.ln = &leaseTestListener{closed: make(chan struct{})}
		type leaseCall struct {
			runID string
			ttl   time.Duration
		}
		acquired := make(chan leaseCall, 1)
		renewed := make(chan leaseCall, 2)
		attempts := 0
		g.opts.locator = &leaseTestLocator{
			acquire: func(_ context.Context, id string, ttl time.Duration) error {
				acquired <- leaseCall{id, ttl}
				return nil
			},
			renew: func(_ context.Context, id string, ttl time.Duration) (bool, error) {
				renewed <- leaseCall{id, ttl}
				attempts++
				if attempts == 1 {
					return false, errors.New("temporary Redis failure")
				}
				return false, nil
			},
		}

		done := make(chan error, 1)
		go func() {
			done <- g.Start(meta.NewContext(t.Context(), testAppInfo{id: "gate-1", name: "gate"}))
		}()
		defer func() { require.NoError(t, g.Stop(context.Background())) }()
		<-g.Ready()
		want := leaseCall{g.runID, gateLeaseTTL}
		require.NotEmpty(t, g.runID)
		require.Equal(t, want, <-acquired)
		require.Empty(t, renewed)

		time.Sleep(gateLeaseInterval)
		synctest.Wait()
		require.Len(t, renewed, 1)
		require.Equal(t, want, <-renewed)
		require.Empty(t, done)

		time.Sleep(gateLeaseInterval)
		synctest.Wait()
		require.Len(t, renewed, 1)
		require.Equal(t, want, <-renewed)
		require.ErrorIs(t, <-done, errGateLeaseLost)
		<-g.stopDone
		time.Sleep(gateLeaseInterval)
		require.Empty(t, renewed)
	})
}

func TestGateStopsWhenRenewalCannotBeConfirmed(t *testing.T) {
	for _, tc := range []struct {
		name         string
		firstRenewed bool
		usableWindow time.Duration
	}{
		{"initial lease", false, gateLeaseTTL},
		{"renewed lease", true, gateLeaseTTL + gateLeaseInterval},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := newLifecycleGate(&testBroker{})
				g.ln = &leaseTestListener{closed: make(chan struct{})}
				first := tc.firstRenewed
				g.opts.locator = &leaseTestLocator{
					acquire: func(context.Context, string, time.Duration) error { return nil },
					renew: func(ctx context.Context, _ string, _ time.Duration) (bool, error) {
						if first {
							first = false
							return true, nil
						}
						<-ctx.Done()
						return false, ctx.Err()
					},
				}
				done := make(chan error, 1)
				go func() {
					done <- g.Start(meta.NewContext(t.Context(), testAppInfo{id: "gate-1", name: "gate"}))
				}()
				defer func() { require.NoError(t, g.Stop(context.Background())) }()
				<-g.Ready()
				time.Sleep(tc.usableWindow - time.Millisecond)
				synctest.Wait()
				require.Empty(t, done)

				time.Sleep(time.Millisecond)
				synctest.Wait()
				select {
				case err := <-done:
					require.ErrorIs(t, err, errGateLeaseLost)
				default:
					t.Fatal("Gate was still serving after its last confirmed lease expired")
				}
				<-g.stopDone
			})
		})
	}
}
