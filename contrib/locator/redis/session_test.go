package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisserver "github.com/alicebob/miniredis/v2/server"
	goredis "github.com/redis/go-redis/v9"

	"github.com/byteweap/meta/component/locator"
)

func newSessionTestLocator(t *testing.T) (*Locator, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	l := newWith(goredis.NewClient(&goredis.Options{Addr: server.Addr()}), "test")
	t.Cleanup(func() { _ = l.Close() })
	return l, server
}

func assertSession(t *testing.T, l *Locator, uid int64, service string, want locator.Session) {
	t.Helper()
	got, err := l.Session(context.Background(), uid, service)
	if err != nil || got != want {
		t.Fatalf("Session = %+v, %v; want %+v", got, err, want)
	}
}

// sessionDeadlineConn 记录 HMGET 是否给实际的网络连接设置了调用方期限
type sessionDeadlineConn struct {
	net.Conn
	want time.Time
	seen *atomic.Bool
}

func (c *sessionDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if deadline.Equal(c.want) {
		c.seen.Store(true)
	}
	return c.Conn.SetReadDeadline(deadline)
}

func TestSessionUsesCallerDeadline(t *testing.T) {
	server := miniredis.RunT(t)
	deadline := time.Now().Add(10 * time.Second)
	var seen atomic.Bool
	l := New(goredis.UniversalOptions{
		Addrs: []string{server.Addr()}, ReadTimeout: time.Minute,
		Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			var dialer net.Dialer
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &sessionDeadlineConn{Conn: conn, want: deadline, seen: &seen}, nil
		},
	}, "test")
	t.Cleanup(func() { _ = l.Close() })
	assertSession(t, l, 7, "gate", locator.Session{})
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	if _, err := l.Session(ctx, 7, "gate"); err != nil {
		t.Fatal(err)
	}
	if !seen.Load() {
		t.Fatal("HMGET did not apply the caller's deadline to its socket read")
	}
}

func TestSessionReadsPrimaryDespiteReplicaOptions(t *testing.T) {
	primary, replica := miniredis.RunT(t), miniredis.RunT(t)
	primary.HSet("test:locator:7", "node:gate", "gate-current", "session:gate", "current")
	replica.HSet("test:locator:7", "node:gate", "gate-old", "session:gate", "old")
	primaryAddr, replicaAddr := primary.Server().Addr(), replica.Server().Addr()
	for _, node := range []*miniredis.Miniredis{primary, replica} {
		node.Server().SetPreHook(func(peer *redisserver.Peer, cmd string, args ...string) bool {
			if cmd == "READONLY" {
				peer.WriteOK()
				return true
			}
			if cmd != "CLUSTER" || len(args) != 1 || strings.ToUpper(args[0]) != "SLOTS" {
				return false
			}
			peer.WriteLen(1)
			peer.WriteLen(4)
			peer.WriteInt(0)
			peer.WriteInt(16383)
			for i, addr := range []*net.TCPAddr{primaryAddr, replicaAddr} {
				peer.WriteLen(3)
				peer.WriteBulk(addr.IP.String())
				peer.WriteInt(addr.Port)
				peer.WriteBulk(fmt.Sprintf("node-%d", i))
			}
			return true
		})
	}
	// Confirm the fixture exposes the stale replica through go-redis's read routing.
	unsafeOptions := goredis.UniversalOptions{Addrs: []string{primary.Addr(), replica.Addr()}, ReadOnly: true}
	unsafeLocator := newWith(goredis.NewUniversalClient(&unsafeOptions), "test")
	t.Cleanup(func() { _ = unsafeLocator.Close() })
	assertSession(t, unsafeLocator, 7, "gate", locator.Session{ID: "old", Node: "gate-old"})

	for name, opts := range map[string]goredis.UniversalOptions{
		"read only": {ReadOnly: true},
		"latency":   {RouteByLatency: true},
		"random":    {RouteRandomly: true},
		"all":       {ReadOnly: true, RouteByLatency: true, RouteRandomly: true},
	} {
		t.Run(name, func(t *testing.T) {
			opts.Addrs = []string{primary.Addr(), replica.Addr()}
			l := New(opts, "test")
			t.Cleanup(func() { _ = l.Close() })
			clientOpts := l.rc.(*goredis.ClusterClient).Options()
			if clientOpts.ReadOnly || clientOpts.RouteByLatency || clientOpts.RouteRandomly {
				t.Fatal("session reads may still be routed to a replica")
			}
			assertSession(t, l, 7, "gate", locator.Session{ID: "current", Node: "gate-current"})
		})
	}
}

func TestPlainBindingHasNoSession(t *testing.T) {
	l, _ := newSessionTestLocator(t)
	ctx := context.Background()
	if err := l.Bind(ctx, 7, "game", "game-1"); err != nil {
		t.Fatal(err)
	}
	assertSession(t, l, 7, "game", locator.Session{})
	if node, err := l.Node(ctx, 7, "game"); err != nil || node != "game-1" {
		t.Fatalf("plain Node = %q, %v", node, err)
	}
}

func TestSessionClaimRelease(t *testing.T) {
	for _, nextNode := range []string{"gate-1", "gate-2"} {
		t.Run(nextNode, func(t *testing.T) {
			l, _ := newSessionTestLocator(t)
			ctx := context.Background()
			assertSession(t, l, 7, "gate", locator.Session{})
			if released, err := l.Release(ctx, 7, "gate", "gate-1", "old"); err != nil || released {
				t.Fatalf("missing Release = %v, %v", released, err)
			}
			if replaced, err := l.Claim(ctx, 7, "gate", "gate-1", "old"); err != nil || replaced {
				t.Fatalf("initial Claim = %v, %v", replaced, err)
			}
			if replaced, err := l.Claim(ctx, 7, "gate", nextNode, "new"); err != nil || !replaced {
				t.Fatalf("reconnect Claim = %v, %v", replaced, err)
			}
			for _, owner := range []locator.Session{{ID: "old", Node: "gate-1"}, {ID: "new", Node: "wrong-node"}} {
				if released, err := l.Release(ctx, 7, "gate", owner.Node, owner.ID); err != nil || released {
					t.Fatalf("stale Release(%+v) = %v, %v", owner, released, err)
				}
			}
			assertSession(t, l, 7, "gate", locator.Session{ID: "new", Node: nextNode})
			for _, wantReleased := range []bool{true, false} {
				if released, err := l.Release(ctx, 7, "gate", nextNode, "new"); err != nil || released != wantReleased {
					t.Fatalf("Release = %v, %v; want %v", released, err, wantReleased)
				}
			}
			assertSession(t, l, 7, "gate", locator.Session{ID: "new"})
			if node, err := l.Node(ctx, 7, "gate"); err != nil || node != "" {
				t.Fatalf("offline Node = %q, %v", node, err)
			}
			if replaced, err := l.Claim(ctx, 7, "gate", "gate-3", "latest"); err != nil || replaced {
				t.Fatalf("Claim after offline = %v, %v", replaced, err)
			}
			if released, err := l.Release(ctx, 7, "gate", nextNode, "new"); err != nil || released {
				t.Fatalf("old offline Release = %v, %v", released, err)
			}
			assertSession(t, l, 7, "gate", locator.Session{ID: "latest", Node: "gate-3"})
		})
	}
}

func TestAllNodesSeparatesRoutingFromSessionState(t *testing.T) {
	l, server := newSessionTestLocator(t)
	ctx := context.Background()
	for service, node := range map[string]string{"game": "game-1", "session:gate": "game-2"} {
		if err := l.Bind(ctx, 7, service, node); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := l.Claim(ctx, 7, "gate", "gate-1", "session-7"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Claim(ctx, 8, "gate", "gate-8", "session-8"); err != nil {
		t.Fatal(err)
	}
	// Historical fields are not interpreted as either a route or a session.
	server.HSet(l.key(7), "legacy-service", "legacy-node")
	want := map[string]string{"game": "game-1", "session:gate": "game-2", "gate": "gate-1"}
	if nodes, err := l.AllNodes(ctx, 7); err != nil || !reflect.DeepEqual(nodes, want) {
		t.Fatalf("AllNodes = %v, %v; want %v", nodes, err, want)
	}
	if _, err := l.Release(ctx, 7, "gate", "gate-1", "session-7"); err != nil {
		t.Fatal(err)
	}
	delete(want, "gate")
	if nodes, err := l.AllNodes(ctx, 7); err != nil || !reflect.DeepEqual(nodes, want) {
		t.Fatalf("offline AllNodes = %v, %v; want %v", nodes, err, want)
	}
	assertSession(t, l, 8, "gate", locator.Session{ID: "session-8", Node: "gate-8"})
}

func TestSessionRejectsUnversionedMutation(t *testing.T) {
	l, _ := newSessionTestLocator(t)
	ctx := context.Background()
	if err := l.Bind(ctx, 7, "gate", "plain-node"); err != nil {
		t.Fatal(err)
	}
	if replaced, err := l.Claim(ctx, 7, "gate", "gate-1", "session-1"); err != nil || !replaced {
		t.Fatalf("Claim over plain binding = %v, %v", replaced, err)
	}
	for _, online := range []bool{true, false} {
		if !online {
			if _, err := l.Release(ctx, 7, "gate", "gate-1", "session-1"); err != nil {
				t.Fatal(err)
			}
		}
		for _, mutate := range []func(context.Context, int64, string, string) error{l.Bind, l.UnBind} {
			if err := mutate(ctx, 7, "gate", "gate-1"); err == nil || !strings.Contains(err.Error(), "requires Claim/Release") {
				t.Fatalf("unversioned mutation while online=%v: %v", online, err)
			}
		}
		want := locator.Session{ID: "session-1"}
		if online {
			want.Node = "gate-1"
		}
		assertSession(t, l, 7, "gate", want)
	}
}

func TestConcurrentClaimsReleaseOnlyCurrentSession(t *testing.T) {
	l, _ := newSessionTestLocator(t)
	ctx := context.Background()
	const count = 12
	var workers sync.WaitGroup
	results := make(chan error, count)
	for i := range count {
		workers.Go(func() {
			_, err := l.Claim(ctx, 7, "gate", fmt.Sprintf("gate-%d", i), fmt.Sprintf("session-%d", i))
			results <- err
		})
	}
	workers.Wait()
	for range count {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	current, err := l.Session(ctx, 7, "gate")
	if err != nil || current.ID == "" || strings.TrimPrefix(current.Node, "gate-") != strings.TrimPrefix(current.ID, "session-") {
		t.Fatalf("inconsistent session: %+v, %v", current, err)
	}
	releases := make(chan bool, count)
	for i := range count {
		workers.Go(func() {
			released, err := l.Release(ctx, 7, "gate", fmt.Sprintf("gate-%d", i), fmt.Sprintf("session-%d", i))
			releases <- released
			results <- err
		})
	}
	workers.Wait()
	releasedCount := 0
	for range count {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		if <-releases {
			releasedCount++
		}
	}
	if releasedCount != 1 {
		t.Fatalf("released %d sessions; want 1", releasedCount)
	}
	assertSession(t, l, 7, "gate", locator.Session{ID: current.ID})
}

// Delay the response to the first successful Redis command in UnBind. A split
// read/delete would then delete the replacement written while the response waits.
type delayedResponseHook struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *delayedResponseHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }
func (h *delayedResponseHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}
func (h *delayedResponseHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil {
			h.once.Do(func() {
				close(h.entered)
				select {
				case <-h.release:
				case <-ctx.Done():
				}
			})
		}
		return err
	}
}

func TestUnBindPreservesConcurrentReplacement(t *testing.T) {
	l, server := newSessionTestLocator(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := l.Bind(ctx, 7, "game", "old"); err != nil {
		t.Fatal(err)
	}
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	hook := &delayedResponseHook{entered: make(chan struct{}), release: make(chan struct{})}
	client.AddHook(hook)
	result := make(chan error, 1)
	go func() { result <- newWith(client, "test").UnBind(ctx, 7, "game", "old") }()
	select {
	case <-hook.entered:
	case <-ctx.Done():
		t.Fatal("UnBind did not reach Redis")
	}
	if err := l.Bind(ctx, 7, "game", "new"); err != nil {
		t.Fatal(err)
	}
	close(hook.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if node, err := l.Node(ctx, 7, "game"); err != nil || node != "new" {
		t.Fatalf("replacement was lost: Node = %q, %v", node, err)
	}
	if err := l.UnBind(ctx, 7, "game", "old"); err != nil {
		t.Fatal(err)
	}
	if err := l.UnBind(ctx, 7, "game", "new"); err != nil {
		t.Fatal(err)
	}
	if node, err := l.Node(ctx, 7, "game"); err != nil || node != "" {
		t.Fatalf("matching UnBind failed: %q, %v", node, err)
	}
}

func TestLocatorRejectsEmptyFields(t *testing.T) {
	l, server := newSessionTestLocator(t)
	ctx := context.Background()
	for _, args := range [][3]string{{"", "node", "session"}, {"gate", "", "session"}, {"gate", "node", ""}} {
		if _, err := l.Claim(ctx, 7, args[0], args[1], args[2]); err == nil {
			t.Errorf("Claim accepted %q", args)
		}
		if _, err := l.Release(ctx, 7, args[0], args[1], args[2]); err == nil {
			t.Errorf("Release accepted %q", args)
		}
	}
	for _, args := range [][2]string{{"", "node"}, {"game", ""}} {
		if err := l.Bind(ctx, 7, args[0], args[1]); err == nil {
			t.Errorf("Bind accepted %q", args)
		}
		if err := l.UnBind(ctx, 7, args[0], args[1]); err == nil {
			t.Errorf("UnBind accepted %q", args)
		}
	}
	if _, err := l.Node(ctx, 7, ""); err == nil {
		t.Error("Node accepted empty service")
	}
	if _, err := l.Session(ctx, 7, ""); err == nil {
		t.Error("Session accepted empty service")
	}
	if keys := server.Keys(); len(keys) != 0 {
		t.Fatalf("invalid arguments mutated Redis: %v", keys)
	}
}

func TestSessionReportsRedisFailure(t *testing.T) {
	l, server := newSessionTestLocator(t)
	server.SetError("ERR unavailable")
	ctx := context.Background()
	if _, err := l.Session(ctx, 7, "gate"); err == nil {
		t.Error("Session swallowed Redis failure")
	}
	if changed, err := l.Claim(ctx, 7, "gate", "node", "session"); err == nil || changed {
		t.Fatalf("Claim = %v, %v on Redis failure", changed, err)
	}
	if changed, err := l.Release(ctx, 7, "gate", "node", "session"); err == nil || changed {
		t.Fatalf("Release = %v, %v on Redis failure", changed, err)
	}
	server.SetError("")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Session(canceled, 7, "gate"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Session ignored cancellation: %v", err)
	}
}

var _ goredis.Hook = (*delayedResponseHook)(nil)
