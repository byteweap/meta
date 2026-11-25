package rpc

import (
	"net/http"
	"testing"

	"github.com/byteweap/meta/examples/game/internal/pb"
)

func TestHelloRespondsSuccessfully(t *testing.T) {
	data, message, status := New(nil).Hello(nil, &pb.FindRoomRequest{RoomId: 7})
	if string(data) != "Hello RPC" || message != "" || status != http.StatusOK {
		t.Fatalf("response = %q, %q, %d", data, message, status)
	}
}
