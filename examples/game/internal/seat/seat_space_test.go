package seat_test

import (
	"math"
	"testing"

	"github.com/byteweap/meta/examples/game/internal/seat"
)

func requireSeat(t *testing.T, space *seat.Space, id int, want *seat.Seat) {
	t.Helper()
	got, ok := space.GetSeat(id)
	if got != want || ok != (want != nil) {
		t.Fatalf("GetSeat(%d) = (%v, %v), want (%v, %v)", id, got, ok, want, want != nil)
	}
}

func TestSpaceCapacity(t *testing.T) {
	for _, size := range []int{-1, math.MinInt} {
		if space, err := seat.NewSpace(size); space != nil || err == nil {
			t.Fatalf("NewSpace(%d) = (%v, %v), want nil and an error", size, space, err)
		}
	}
	for _, size := range []int{0, 1, 3} {
		space, err := seat.NewSpace(size)
		if err != nil || space == nil {
			t.Fatalf("NewSpace(%d) = (%v, %v)", size, space, err)
		}
		for id := range size {
			requireSeat(t, space, id, nil)
			value := seat.New(id)
			if err := space.Register(value); err != nil {
				t.Fatalf("register seat %d in capacity %d: %v", id, size, err)
			}
			requireSeat(t, space, id, value)
		}
		requireSeat(t, space, size, nil)
		if err := space.Register(seat.New(size)); err == nil {
			t.Fatalf("capacity %d accepted seat %d", size, size)
		}
	}
}

func TestSpaceRegistrationLifecycle(t *testing.T) {
	space, err := seat.NewSpace(3)
	if err != nil {
		t.Fatal(err)
	}
	first, last := seat.New(0), seat.New(2)
	for _, value := range []*seat.Seat{first, last} {
		if err := space.Register(value); err != nil {
			t.Fatal(err)
		}
	}
	requireSeat(t, space, 0, first)
	requireSeat(t, space, 1, nil)
	requireSeat(t, space, 2, last)

	space.Unregister(1) // Removing an empty slot leaves other seats intact.
	space.Unregister(0)
	space.Unregister(0)
	requireSeat(t, space, 0, nil)
	requireSeat(t, space, 2, last)

	replacement := seat.New(0)
	if err := space.Register(replacement); err != nil {
		t.Fatalf("register after removal: %v", err)
	}
	requireSeat(t, space, 0, replacement)
	requireSeat(t, space, 2, last)
	space.Unregister(2)
	requireSeat(t, space, 2, nil)
	requireSeat(t, space, 0, replacement)
	if err := space.Register(last); err != nil {
		t.Fatalf("register last seat after removal: %v", err)
	}
	requireSeat(t, space, 2, last)
}

func TestSpaceInvalidInputsPreserveState(t *testing.T) {
	space, err := seat.NewSpace(2)
	if err != nil {
		t.Fatal(err)
	}
	first, last := seat.New(0), seat.New(1)
	for _, value := range []*seat.Seat{first, last} {
		if err := space.Register(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := space.Register(nil); err == nil {
		t.Fatal("nil seat accepted")
	}
	for _, id := range []int{math.MinInt, -1, 2, math.MaxInt} {
		requireSeat(t, space, id, nil)
		if err := space.Register(seat.New(id)); err == nil {
			t.Fatalf("invalid seat ID %d accepted", id)
		}
		space.Unregister(id)
		requireSeat(t, space, 0, first)
		requireSeat(t, space, 1, last)
	}

	var empty seat.Space
	for _, id := range []int{math.MinInt, -1, 0, math.MaxInt} {
		requireSeat(t, &empty, id, nil)
		if err := empty.Register(seat.New(id)); err == nil {
			t.Fatalf("zero-value space accepted seat %d", id)
		}
		empty.Unregister(id)
	}
}

func TestSpaceDuplicateRegistrationPreservesSeat(t *testing.T) {
	space, err := seat.NewSpace(1)
	if err != nil {
		t.Fatal(err)
	}
	original := seat.New(0)
	if err := space.Register(original); err != nil {
		t.Fatal(err)
	}
	for _, duplicate := range []*seat.Seat{original, seat.New(0)} {
		if err := space.Register(duplicate); err == nil {
			t.Fatal("duplicate seat accepted")
		}
		requireSeat(t, space, 0, original)
	}
}

func TestNewSeatIsUnoccupied(t *testing.T) {
	value := seat.New(3)
	if value.ID() != 3 || value.Player() != nil {
		t.Fatalf("new seat ID=%d player=%v", value.ID(), value.Player())
	}
}
