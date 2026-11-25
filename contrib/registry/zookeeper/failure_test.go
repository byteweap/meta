package zookeeper

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-zookeeper/zk"

	"github.com/byteweap/meta/component/registry"
)

type testConnection struct {
	exists    func(string) (bool, *zk.Stat, error)
	create    func(string, []byte, int32, []zk.ACL) (string, error)
	delete    func(string, int32) error
	children  func(string) ([]string, *zk.Stat, error)
	get       func(string) ([]byte, *zk.Stat, error)
	childrenW func(string) ([]string, *zk.Stat, <-chan zk.Event, error)
	existsW   func(string) (bool, *zk.Stat, <-chan zk.Event, error)
	sessionID func() int64
}

func (c testConnection) Exists(path string) (bool, *zk.Stat, error) {
	return c.exists(path)
}

func (c testConnection) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	return c.create(path, data, flags, acl)
}

func (c testConnection) Delete(path string, version int32) error { return c.delete(path, version) }

func (c testConnection) Children(path string) ([]string, *zk.Stat, error) { return c.children(path) }

func (c testConnection) Get(path string) ([]byte, *zk.Stat, error) { return c.get(path) }

func (c testConnection) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	return c.childrenW(path)
}

func (c testConnection) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	return c.existsW(path)
}

func (c testConnection) SessionID() int64 { return c.sessionID() }

func TestRegisterStopsAtFirstBackendError(t *testing.T) {
	failure := errors.New("ZooKeeper operation failed")
	for _, stage := range []string{"/services", "/services/game", "/services/game/node", "delete", "create"} {
		t.Run(stage, func(t *testing.T) {
			conn := testConnection{
				exists: func(key string) (bool, *zk.Stat, error) {
					if key == stage {
						return false, nil, failure
					}
					return true, new(zk.Stat), nil
				},
				delete: func(string, int32) error {
					if stage == "delete" {
						return failure
					}
					return zk.ErrNoNode
				},
				create: func(string, []byte, int32, []zk.ACL) (string, error) { return "", failure },
			}
			r := &Registry{opts: &options{namespace: "/services"}, conn: conn}
			svc := &registry.ServiceInstance{Name: "game", ID: "node"}
			if r.ID() != "zookeeper" || !errors.Is(r.Register(t.Context(), svc), failure) {
				t.Fatalf("registration lost error in %s", stage)
			}
		})
	}
}

func TestLookupAndWatchPreserveErrorsAndFilterOtherServices(t *testing.T) {
	failure := errors.New("ZooKeeper read failed")
	for _, stage := range []string{"children", "get", "decode", "filter"} {
		t.Run(stage, func(t *testing.T) {
			conn := testConnection{
				children: func(string) ([]string, *zk.Stat, error) {
					if stage == "children" {
						return nil, nil, failure
					}
					return []string{"old-name"}, nil, nil
				},
				get: func(string) ([]byte, *zk.Stat, error) {
					switch stage {
					case "get":
						return nil, nil, failure
					case "decode":
						return []byte("{"), nil, nil
					default:
						return []byte(`{"Name":"other","ID":"old-name"}`), nil, nil
					}
				},
			}
			r := &Registry{opts: &options{namespace: "/services"}, conn: conn}
			w := &watcher{serviceName: "game", prefix: "/services/game", conn: conn}
			got, err := w.Next()
			if stage == "filter" {
				if err != nil || len(got) != 0 {
					t.Fatalf("watch returned mismatched service: %v, %v", got, err)
				}
				return
			}
			for _, call := range []func() ([]*registry.ServiceInstance, error){
				w.getServices, func() ([]*registry.ServiceInstance, error) { return r.GetService(t.Context(), "game") },
			} {
				got, err := call()
				if got != nil || err == nil || (stage != "decode" && !errors.Is(err, failure)) {
					t.Fatalf("read = %v, %v in %s", got, err, stage)
				}
			}
		})
	}
}

func TestWatcherWaitsForMissingNodeAndReportsEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exists := make(chan zk.Event, 1)
		pending := make(chan zk.Event)
		failure := errors.New("watch event failed")
		childrenCalls, existsCalls := 0, 0
		conn := testConnection{
			childrenW: func(string) ([]string, *zk.Stat, <-chan zk.Event, error) {
				childrenCalls++
				if childrenCalls == 1 {
					return nil, nil, nil, zk.ErrNoNode
				}
				return nil, nil, pending, nil
			},
			existsW: func(string) (bool, *zk.Stat, <-chan zk.Event, error) {
				existsCalls++
				return false, nil, exists, nil
			},
			children: func(string) ([]string, *zk.Stat, error) { return nil, nil, nil },
		}
		w, err := newWatcher(t.Context(), "/services/game", "game", conn)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = w.Stop() }()
		if got, err := w.Next(); len(got) != 0 || err != nil {
			t.Fatalf("initial snapshot = %v, %v", got, err)
		}
		exists <- zk.Event{State: zk.StateHasSession, Err: failure}
		if got, err := w.Next(); got != nil || !errors.Is(err, failure) {
			t.Fatalf("event snapshot = %v, %v", got, err)
		}
		synctest.Wait()
		if existsCalls != 1 || childrenCalls != 2 {
			t.Fatalf("existence queries=%d child queries=%d", existsCalls, childrenCalls)
		}
	})
}

func TestReregisterRestoresEphemeralNodeOnSessionChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("cannot restore node twice")
		const key = "/services/game/node"
		call, creates := int64(0), 0
		conn := testConnection{
			sessionID: func() int64 {
				call++
				return call
			},
			exists: func(string) (bool, *zk.Stat, error) { return false, new(zk.Stat), nil },
			delete: func(string, int32) error { return zk.ErrNoNode },
			create: func(got string, data []byte, flags int32, _ []zk.ACL) (string, error) {
				creates++
				if got != key || string(data) != "record" || flags != zk.FlagEphemeral {
					t.Fatalf("recreated node = %q %q %v", got, data, flags)
				}
				if creates == 2 {
					return "", failure
				}
				return got, nil
			},
		}
		r := Registry{opts: &options{}, conn: conn}
		start := time.Now()
		r.reRegister(key, []byte("record"))
		if creates != 2 || time.Since(start) != 2*time.Second {
			t.Fatalf("recreated node %d times over %s", creates, time.Since(start))
		}
	})
}
