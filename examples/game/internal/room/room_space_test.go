package room

import "testing"

func TestSpaceTracksRoomLifecycle(t *testing.T) {
	const id = 42
	space := NewSpace()
	if got, ok := space.GetRoom(id); got != nil || ok || space.NumRooms() != 0 {
		t.Fatalf("empty space returned %v, %v and count %d", got, ok, space.NumRooms())
	}
	r := New(id)
	space.Register(r)
	if got, ok := space.GetRoom(id); got != r || !ok || space.NumRooms() != 1 {
		t.Fatalf("registered space returned %v, %v and count %d", got, ok, space.NumRooms())
	}
	space.Unregister(id)
	if got, ok := space.GetRoom(id); got != nil || ok || space.NumRooms() != 0 {
		t.Fatalf("space after removal returned %v, %v and count %d", got, ok, space.NumRooms())
	}
}
