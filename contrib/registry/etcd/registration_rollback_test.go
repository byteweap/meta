package etcd

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type rollbackLease struct {
	clientv3.Lease
	grant  func(context.Context, int64) (*clientv3.LeaseGrantResponse, error)
	revoke func(context.Context, clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
}

func (l *rollbackLease) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return l.grant(ctx, ttl)
}

func (l *rollbackLease) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return l.revoke(ctx, id)
}

type rollbackKV struct {
	clientv3.KV
	put func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)
}

func (kv *rollbackKV) Put(ctx context.Context, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return kv.put(ctx, key, value, opts...)
}

func TestRegisterWithKVRollsBackOnlyGrantedLease(t *testing.T) {
	grantErr, putErr, revokeErr := errors.New("grant failed"), errors.New("put failed"), errors.New("revoke failed")
	for _, stage := range []string{"grant", "put", "revoke", "success"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var writes, revokes int
			r := &Registry{opts: &options{ttl: 15 * time.Second}}
			r.lease = &rollbackLease{
				grant: func(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
					if stage == "grant" {
						return nil, grantErr
					}
					return &clientv3.LeaseGrantResponse{ID: 42}, nil
				},
				revoke: func(cleanupCtx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
					revokes++
					if id != 42 || cleanupCtx.Err() != nil {
						t.Fatalf("invalid cleanup: lease=%d err=%v", id, cleanupCtx.Err())
					}
					if _, ok := cleanupCtx.Deadline(); !ok {
						t.Fatal("cleanup has no deadline")
					}
					if stage == "revoke" {
						return nil, revokeErr
					}
					return &clientv3.LeaseRevokeResponse{}, nil
				},
			}
			r.kv = &rollbackKV{put: func(_ context.Context, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
				writes++
				if !reflect.DeepEqual(clientv3.OpPut(key, value, opts...), clientv3.OpPut("service/key", "instance", clientv3.WithLease(42))) {
					t.Fatal("write did not use the granted lease")
				}
				cancel() // Cleanup must survive cancellation, including a successful final response.
				if stage == "success" {
					return &clientv3.PutResponse{}, nil
				}
				return nil, putErr
			}}
			id, err := r.registerWithKV(ctx, "service/key", "instance")
			switch stage {
			case "grant":
				if !errors.Is(err, grantErr) || writes != 0 || revokes != 0 {
					t.Fatalf("failed grant caused side effects: writes=%d revokes=%d err=%v", writes, revokes, err)
				}
			case "success":
				if err != nil || id != 42 || revokes != 0 {
					t.Fatalf("successful write was rolled back: id=%d revokes=%d err=%v", id, revokes, err)
				}
			default:
				if id != 0 || revokes != 1 || !errors.Is(err, putErr) || (stage == "revoke" && !errors.Is(err, revokeErr)) {
					t.Fatalf("failed write lost errors or leaked its lease: id=%d revokes=%d err=%v", id, revokes, err)
				}
			}
		})
	}
}
