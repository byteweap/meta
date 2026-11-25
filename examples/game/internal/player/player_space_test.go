package player

import "testing"

func TestSpaceTracksPlayerLifecycle(t *testing.T) {
	const id = 42
	space := NewSpace()
	if got, ok := space.GetPlayer(id); got != nil || ok || space.NumPlayers() != 0 {
		t.Fatalf("empty space returned %v, %v and count %d", got, ok, space.NumPlayers())
	}
	p := New(id)
	space.Register(p)
	if got, ok := space.GetPlayer(id); got != p || !ok || p.ID() != id || space.NumPlayers() != 1 {
		t.Fatalf("registered space returned %v, %v and count %d", got, ok, space.NumPlayers())
	}
	space.Unregister(id)
	if got, ok := space.GetPlayer(id); got != nil || ok || space.NumPlayers() != 0 {
		t.Fatalf("space after removal returned %v, %v and count %d", got, ok, space.NumPlayers())
	}
}
