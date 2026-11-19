package redis

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	redisserver "github.com/alicebob/miniredis/v2/server"
	goredis "github.com/redis/go-redis/v9"

	"github.com/byteweap/meta/component/locator"
)

func TestGateRunLeaseExpiresAllItsSessions(t *testing.T) {
	l, server := newSessionTestLocator(t)
	ctx := t.Context()
	const sameNode = "gate-1"
	for _, uid := range []int64{7, 8} {
		id := fmt.Sprint("session-", uid)
		if _, err := l.Claim(ctx, uid, "gate", sameNode, id, testGateRun); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Bind(ctx, 7, "game", "game-1"); err != nil {
		t.Fatal(err)
	}
	server.FastForward(15 * time.Second)
	if renewed, err := l.RenewGateLease(ctx, testGateRun, time.Minute); err != nil || !renewed {
		t.Fatalf("RenewGateLease = %v, %v", renewed, err)
	}
	server.FastForward(45 * time.Second)
	assertSession(t, l, 7, "gate", locator.Session{ID: "session-7", Node: sameNode})
	assertSession(t, l, 8, "gate", locator.Session{ID: "session-8", Node: sameNode})

	server.FastForward(15 * time.Second)
	assertSession(t, l, 7, "gate", locator.Session{ID: "session-7"})
	assertSession(t, l, 8, "gate", locator.Session{ID: "session-8"})
	if renewed, err := l.RenewGateLease(ctx, testGateRun, time.Minute); err != nil || renewed {
		t.Fatalf("expired RenewGateLease = %v, %v", renewed, err)
	}
	assertSession(t, l, 7, "gate", locator.Session{ID: "session-7"})
	if node, err := l.Node(ctx, 7, "game"); err != nil || node != "game-1" {
		t.Fatalf("ordinary Node = %q, %v", node, err)
	}
	if nodes, err := l.AllNodes(ctx, 7); err != nil || !reflect.DeepEqual(nodes, map[string]string{"game": "game-1", "gate": sameNode}) {
		t.Fatalf("stored bindings = %v, %v", nodes, err)
	}

	const nextRun = "restarted-gate-run"
	if err := l.AcquireGateLease(ctx, nextRun, time.Minute); err != nil {
		t.Fatal(err)
	}
	assertSession(t, l, 7, "gate", locator.Session{ID: "session-7"})
	if replaced, err := l.Claim(ctx, 7, "gate", sameNode, "new-session", nextRun); err != nil || !replaced {
		t.Fatalf("Claim after restart = %v, %v", replaced, err)
	}
	if replaced, err := l.Claim(ctx, 7, "gate", sameNode, "late-claim", testGateRun); err == nil || replaced {
		t.Fatalf("Claim from expired run = %v, %v", replaced, err)
	}
	if released, err := l.Release(ctx, 7, "gate", sameNode, "session-7"); err != nil || released {
		t.Fatalf("stale Release = %v, %v", released, err)
	}
	assertSession(t, l, 7, "gate", locator.Session{ID: "new-session", Node: sameNode})
	assertSession(t, l, 8, "gate", locator.Session{ID: "session-8"})
}

func TestSessionPropagatesGateLeaseReadFailure(t *testing.T) {
	l, server := newSessionTestLocator(t)
	ctx := t.Context()
	if _, err := l.Claim(ctx, 7, "gate", "gate-1", "current", testGateRun); err != nil {
		t.Fatal(err)
	}
	// 制造明确的存储错误，验证查询不会把租约读取失败解释成离线
	server.Server().SetPreHook(func(peer *redisserver.Peer, cmd string, _ ...string) bool {
		if cmd != "EXISTS" {
			return false
		}
		peer.WriteError("ERR lease storage unavailable")
		return true
	})
	defer server.Server().SetPreHook(nil)
	if _, err := l.Session(ctx, 7, "gate"); err == nil || !strings.Contains(err.Error(), "lease storage unavailable") {
		t.Fatalf("Session lease read error = %v", err)
	}
}

func TestClaimFromExpiredGateCannotReplaceNewSession(t *testing.T) {
	l, server := newSessionTestLocator(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := l.AcquireGateLease(ctx, "next-run", 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	hook := &delayedResponseHook{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		match: func(cmd goredis.Cmder) bool {
			return cmd.Name() == "exists" && cmd.Args()[1] == l.gateLeaseKey(testGateRun)
		},
	}
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	client.AddHook(hook)
	unblock := sync.OnceFunc(func() { close(hook.release) })
	defer unblock()
	oldClaim := make(chan error, 1)
	go func() {
		old := newWith(client, "test")
		_, err := old.Claim(ctx, 7, "gate", "gate-1", "old-session", testGateRun)
		oldClaim <- err
	}()
	select {
	case <-hook.entered:
	case <-ctx.Done():
		t.Fatal("old Claim did not reach the lease check")
	}
	server.FastForward(time.Minute)
	if _, err := l.Claim(ctx, 7, "gate", "gate-1", "new-session", "next-run"); err != nil {
		t.Fatal(err)
	}
	unblock()
	select {
	case err := <-oldClaim:
		if err == nil {
			t.Error("old Claim succeeded after lease expiration and replacement")
		}
	case <-ctx.Done():
		t.Fatal("old Claim did not return")
	}
	assertSession(t, l, 7, "gate", locator.Session{ID: "new-session", Node: "gate-1"})
}

func TestUnleasedSessionIsOffline(t *testing.T) {
	l, server := newSessionTestLocator(t)
	server.HSet(l.key(7), "node:gate", "old-gate", "session:gate", "old-session")
	assertSession(t, l, 7, "gate", locator.Session{ID: "old-session"})
}
