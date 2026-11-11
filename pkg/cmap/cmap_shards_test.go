package cmap

import (
	"encoding/json"
	"maps"
	"math"
	"strconv"
	"sync"
	"testing"
)

func shardTestHash(key int) uint32 { return uint32(key) }

func TestShardConfigurationValidation(t *testing.T) {
	for _, count := range []int{0, -1, math.MinInt} {
		if m, err := New[int, int](shardTestHash, count); m != nil || err == nil {
			t.Fatalf("New with %d shards = (%v, %v), want nil and an error", count, m, err)
		}
	}
	if m, err := New[int, int](nil, DefaultShardCount); m != nil || err == nil {
		t.Fatalf("New with nil hash = (%v, %v), want nil and an error", m, err)
	}
}

func assertShardContents(t *testing.T, m *ConcurrentMap[int, int], want map[int]int) {
	t.Helper()
	if count := m.Count(); count != len(want) {
		t.Fatalf("Count = %d, want %d", count, len(want))
	}
	for key, value := range want {
		if got, ok := m.Get(key); !ok || got != value || !m.Has(key) {
			t.Fatalf("key %d = (%d, %v), want %d", key, got, ok, value)
		}
	}
	if got := m.Items(); !maps.Equal(got, want) {
		t.Fatalf("Items = %v, want %v", got, want)
	}
	keys := m.Keys()
	seen := make(map[int]bool, len(keys))
	for _, key := range keys {
		if _, ok := want[key]; !ok || seen[key] {
			t.Fatalf("unexpected or duplicate key %d", key)
		}
		seen[key] = true
	}
	if len(keys) != len(want) {
		t.Fatalf("Keys returned %d entries, want %d", len(keys), len(want))
	}
	items := make(map[int]int)
	for item := range m.Iter() {
		if _, duplicate := items[item.Key]; duplicate {
			t.Fatalf("Iter duplicated key %d", item.Key)
		}
		items[item.Key] = item.Val
	}
	if !maps.Equal(items, want) {
		t.Fatalf("Iter = %v, want %v", items, want)
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[int]int
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(decoded, want) {
		t.Fatalf("JSON = %s, want %v", data, want)
	}
}

func TestShardConfigurationsStayIndependent(t *testing.T) {
	counts := []int{1, 3, DefaultShardCount, 64}
	instances := make([]*ConcurrentMap[int, int], len(counts))
	expected := make([]map[int]int, len(counts))
	for i, count := range counts {
		m := mustNewMap[int, int](t, shardTestHash, count)
		want := make(map[int]int)
		// Populate each instance before constructing the next one. -1 exercises a large hash.
		for key := -1; key < 96; key++ {
			want[key] = i*1000 + key
			m.Set(key, want[key])
		}
		instances[i], expected[i] = m, want
	}
	for i, m := range instances {
		assertShardContents(t, m, expected[i])
		m.Clear()
		assertShardContents(t, m, nil)
		for j, other := range instances {
			if j != i {
				assertShardContents(t, other, expected[j])
			}
		}
		data, err := json.Marshal(expected[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, m); err != nil {
			t.Fatal(err)
		}
		assertShardContents(t, m, expected[i])
	}
}

func TestShardConfigurationsConcurrent(t *testing.T) {
	for _, count := range []int{1, 3, DefaultShardCount, 64} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			m := mustNewMap[int, int](t, shardTestHash, count)
			const workers, perWorker = 4, 64
			start := make(chan struct{})
			var wg sync.WaitGroup
			for worker := range workers {
				wg.Go(func() {
					<-start
					for key := worker * perWorker; key < (worker+1)*perWorker; key++ {
						value := key*10 + count
						m.Set(key, value)
						if got, ok := m.Get(key); !ok || got != value {
							t.Errorf("Get(%d) = (%d, %v), want %d", key, got, ok, value)
						}
						if key%2 != 0 {
							m.Remove(key)
						}
					}
				})
			}
			wg.Go(func() {
				<-start
				for range 8 {
					// During writes, only check valid entries; no cross-shard snapshot is promised.
					for item := range m.Iter() {
						if item.Key < 0 || item.Key >= workers*perWorker || item.Val != item.Key*10+count {
							t.Errorf("unexpected concurrent entry: %+v", item)
						}
					}
					for _, key := range m.Keys() {
						if key < 0 || key >= workers*perWorker {
							t.Errorf("unexpected concurrent key: %d", key)
						}
					}
					if n := m.Count(); n < 0 || n > workers*perWorker {
						t.Errorf("invalid concurrent count: %d", n)
					}
				}
			})
			close(start)
			wg.Wait()
			want := make(map[int]int)
			for key := 0; key < workers*perWorker; key += 2 {
				want[key] = key*10 + count
			}
			assertShardContents(t, m, want)
		})
	}
}
