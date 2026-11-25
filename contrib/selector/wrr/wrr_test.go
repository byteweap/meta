package wrr

import (
	"errors"
	"reflect"
	"testing"

	"github.com/byteweap/meta/component/selector"
)

func TestWRRSelector_Select_EmptyNodes(t *testing.T) {
	ws := New()
	ws.Update([]selector.Node{})

	got, err := ws.Select("ignored")
	if !errors.Is(err, selector.ErrNoAvailableNode) {
		t.Fatalf("expected ErrNoAvailableNode, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil node for empty nodes, got %#v", got)
	}
}

func TestWRRSelector_UpdateAndNodes(t *testing.T) {
	ws := New()
	nodes := []selector.Node{
		selector.NewNode("node-a", "service1", "v1.0.0", map[string]string{"weight": "1"}),
		selector.NewNode("node-b", "service1", "v1.0.0", map[string]string{"weight": "2"}),
	}

	ws.Update(nodes)
	got := ws.Nodes()
	if len(got) != len(nodes) || got[0].ID() != "node-a" || got[1].ID() != "node-b" {
		t.Fatalf("Nodes() mismatch: got %#v want %#v", got, nodes)
	}
}

func TestWRRSelector_Select_SmoothSequence(t *testing.T) {
	ws := New()
	ws.Update([]selector.Node{
		selector.NewNode("node-a", "service1", "v1.0.0", map[string]string{"weight": "2"}),
		selector.NewNode("node-b", "service1", "v1.0.0", map[string]string{"weight": "1"}),
	})

	want := []string{"node-a", "node-b", "node-a", "node-a", "node-b", "node-a"}
	got := make([]string, 0, len(want))
	for range want {
		n, err := ws.Select("ignored")
		if err != nil {
			t.Fatalf("Select returned error: %v", err)
		}
		got = append(got, n.ID())
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sequence mismatch: got %v want %v", got, want)
	}
}

func TestWRRSelector_UpdateResetsState(t *testing.T) {
	ws := New()
	ws.Update([]selector.Node{
		selector.NewNode("node-a", "service1", "v1.0.0", map[string]string{"weight": "2"}),
		selector.NewNode("node-b", "service1", "v1.0.0", map[string]string{"weight": "1"}),
	})

	for i := 0; i < 3; i++ {
		if _, err := ws.Select("ignored"); err != nil {
			t.Fatalf("Select returned error: %v", err)
		}
	}

	ws.Update([]selector.Node{
		selector.NewNode("node-c", "service1", "v1.0.0", map[string]string{"weight": "1"}),
		selector.NewNode("node-d", "service1", "v1.0.0", map[string]string{"weight": "1"}),
	})

	want := []string{"node-c", "node-d", "node-c", "node-d"}
	got := make([]string, 0, len(want))
	for range want {
		n, err := ws.Select("ignored")
		if err != nil {
			t.Fatalf("Select returned error: %v", err)
		}
		got = append(got, n.ID())
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sequence mismatch after Update: got %v want %v", got, want)
	}
}

func TestWRRSelector_Select_DefaultWeightForNonPositive(t *testing.T) {
	ws := New()
	ws.Update([]selector.Node{
		selector.NewNode("node-a", "service1", "v1.0.0", map[string]string{"weight": "-2"}),
		selector.NewNode("node-b", "service1", "v1.0.0", map[string]string{"weight": "1"}),
	})

	want := []string{"node-a", "node-b", "node-a", "node-b"}
	got := make([]string, 0, len(want))
	for range want {
		n, err := ws.Select("ignored")
		if err != nil {
			t.Fatalf("Select returned error: %v", err)
		}
		got = append(got, n.ID())
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sequence mismatch with non-positive weight: got %v want %v", got, want)
	}
}

func TestWRRSelector_Select_WithFilter(t *testing.T) {
	ws := New()
	ws.Update([]selector.Node{
		selector.NewNode("node-a", "service1", "v1.0.0", map[string]string{"weight": "1"}),
		selector.NewNode("node-b", "service1", "v1.0.0", map[string]string{"weight": "1"}),
		selector.NewNode("node-c", "service1", "v1.0.0", map[string]string{"weight": "1"}),
	})

	onlyB := func(nodes []selector.Node) []selector.Node {
		out := make([]selector.Node, 0, len(nodes))
		for _, n := range nodes {
			if n.ID() == "node-b" {
				out = append(out, n)
			}
		}
		return out
	}

	got, err := ws.Select("ignored", onlyB)
	if err != nil {
		t.Fatalf("Select returned error: %v", err)
	}
	if got.ID() != "node-b" {
		t.Fatalf("expected node-b, got %v", got.ID())
	}
}

func TestFilteredWeightedSequence(t *testing.T) {
	a := selector.NewNode("a", "svc", "1", nil)
	b := selector.NewNode("b", "svc", "1", map[string]string{"weight": "2"})
	c := selector.NewNode("c", "svc", "1", nil)
	other := selector.NewNode("other", "svc", "1", nil)
	ws := New()
	ws.Update([]selector.Node{a, b, c})
	filter := func(nodes []selector.Node) []selector.Node { return append(nodes[:2], other) }
	for _, want := range []selector.Node{b, a, b} {
		got, err := ws.Select("", nil, filter)
		if err != nil || got != want {
			t.Fatalf("filtered selection = %v, %v, want %s", got, err, want.ID())
		}
	}

	for _, filter := range []selector.Filter{
		func([]selector.Node) []selector.Node { return nil },
		func([]selector.Node) []selector.Node { return []selector.Node{other, other} },
	} {
		got, err := ws.Select("", filter)
		if got != nil || !errors.Is(err, selector.ErrNoAvailableNode) {
			t.Fatalf("all filtered = %v, %v", got, err)
		}
	}
}
