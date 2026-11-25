package pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestProtocolMessages(t *testing.T) {
	type legacyMessage interface {
		proto.Message
		Descriptor() ([]byte, []int)
		Reset()
		String() string
	}
	tests := []struct {
		name  string
		msg   legacyMessage
		zero  legacyMessage
		index int
	}{
		{"EnterGameRequest", &EnterGameRequest{Tip: "a"}, (*EnterGameRequest)(nil), 0},
		{"EnterGameResponse", &EnterGameResponse{Tip: "a"}, (*EnterGameResponse)(nil), 1},
		{"ExitGameRequest", &ExitGameRequest{Tip: "a"}, (*ExitGameRequest)(nil), 2},
		{"ExitGameResponse", &ExitGameResponse{Tip: "a"}, (*ExitGameResponse)(nil), 3},
		{"FindRoomRequest", &FindRoomRequest{RoomId: 7}, (*FindRoomRequest)(nil), 0},
		{"FindRoomResponse", &FindRoomResponse{RoomId: 7}, (*FindRoomResponse)(nil), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := protoreflect.FullName("game.v1." + tt.name)
			if tt.msg.ProtoReflect().Descriptor().FullName() != want || tt.zero.ProtoReflect().Descriptor().FullName() != want {
				t.Fatalf("descriptor full name = %v, want %v", tt.msg.ProtoReflect().Descriptor().FullName(), want)
			}
			descriptor, indexes := tt.msg.Descriptor()
			if len(descriptor) == 0 || len(indexes) != 1 || indexes[0] != tt.index || tt.msg.String() == "" {
				t.Fatalf("unexpected descriptor or text for %s: indexes=%v", tt.name, indexes)
			}
			if tip, ok := tt.msg.(interface{ GetTip() string }); ok {
				if tip.GetTip() != "a" || tt.zero.(interface{ GetTip() string }).GetTip() != "" {
					t.Errorf("unexpected tip getter for %s", tt.name)
				}
			} else if tt.msg.(interface{ GetRoomId() int32 }).GetRoomId() != 7 ||
				tt.zero.(interface{ GetRoomId() int32 }).GetRoomId() != 0 {
				t.Errorf("unexpected room ID getter for %s", tt.name)
			}
			tt.msg.Reset()
			data, err := proto.Marshal(tt.msg)
			if err != nil || len(data) != 0 {
				t.Fatalf("reset message marshaled as %v, err=%v", data, err)
			}
		})
	}
}
