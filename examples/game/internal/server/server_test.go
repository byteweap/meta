package server

import (
	"testing"

	"github.com/byteweap/meta/examples/game/internal/player"
	"github.com/byteweap/meta/examples/game/internal/room"
)

func TestNewServerTracksRoomsAndPlayers(t *testing.T) {
	s := New()
	if s.Mesh == nil || s.NumPlayers() != 0 || s.NumRooms() != 0 {
		t.Fatal("new server did not initialize empty spaces and Mesh")
	}
	s.roomSpace.Register(room.New(1))
	s.playerSpace.Register(player.New(42))
	if s.NumPlayers() != 1 || s.NumRooms() != 1 {
		t.Fatalf("server counts = %d players, %d rooms", s.NumPlayers(), s.NumRooms())
	}
}
