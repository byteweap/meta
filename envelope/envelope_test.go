package envelope_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/byteweap/meta/envelope"
)

func TestEnvelopeAccessors(t *testing.T) {
	header := &envelope.Header{Seq: 1, Cmd: 2, Version: 3, SentAtUnixMs: 4}
	failure := &envelope.ResponseError{Code: 5, Message: "rejected"}
	payload := []byte("body")
	request := &envelope.ClientEnvelope{Header: header, Service: "game", Payload: payload}
	response := &envelope.ServerEnvelope{Header: header, Service: "game", Kind: envelope.MsgType_MSG_TYPE_RESPONSE, Error: failure, Payload: payload}

	require.EqualValues(t, 1, header.GetSeq())
	require.EqualValues(t, 2, header.GetCmd())
	require.EqualValues(t, 3, header.GetVersion())
	require.EqualValues(t, 4, header.GetSentAtUnixMs())
	require.EqualValues(t, 5, failure.GetCode())
	require.Equal(t, "rejected", failure.GetMessage())
	require.Same(t, header, request.GetHeader())
	require.Equal(t, "game", request.GetService())
	require.Equal(t, payload, request.GetPayload())
	require.Same(t, header, response.GetHeader())
	require.Equal(t, "game", response.GetService())
	require.Equal(t, envelope.MsgType_MSG_TYPE_RESPONSE, response.GetKind())
	require.Same(t, failure, response.GetError())
	require.Equal(t, payload, response.GetPayload())

	var nilHeader *envelope.Header
	var nilFailure *envelope.ResponseError
	var nilRequest *envelope.ClientEnvelope
	var nilResponse *envelope.ServerEnvelope
	require.Zero(t, nilHeader.GetSeq())
	require.Zero(t, nilHeader.GetCmd())
	require.Zero(t, nilHeader.GetVersion())
	require.Zero(t, nilHeader.GetSentAtUnixMs())
	require.Zero(t, nilFailure.GetCode())
	require.Empty(t, nilFailure.GetMessage())
	require.Nil(t, nilRequest.GetHeader())
	require.Empty(t, nilRequest.GetService())
	require.Nil(t, nilRequest.GetPayload())
	require.Nil(t, nilResponse.GetHeader())
	require.Empty(t, nilResponse.GetService())
	require.Equal(t, envelope.MsgType_MSG_TYPE_UNSPECIFIED, nilResponse.GetKind())
	require.Nil(t, nilResponse.GetError())
	require.Nil(t, nilResponse.GetPayload())
}

func TestEnvelopeDescriptors(t *testing.T) {
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
		{"Header", &envelope.Header{Seq: 9}, (*envelope.Header)(nil), 0},
		{"ClientEnvelope", &envelope.ClientEnvelope{Service: "g"}, (*envelope.ClientEnvelope)(nil), 1},
		{"ServerEnvelope", &envelope.ServerEnvelope{Service: "g"}, (*envelope.ServerEnvelope)(nil), 2},
		{"ResponseError", &envelope.ResponseError{Code: 1}, (*envelope.ResponseError)(nil), 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fullName := protoreflect.FullName("envelope.v1." + tt.name)
			require.Equal(t, fullName, tt.msg.ProtoReflect().Descriptor().FullName())
			require.Equal(t, fullName, tt.zero.ProtoReflect().Descriptor().FullName())
			data, indexes := tt.msg.Descriptor()
			require.NotEmpty(t, data)
			require.Equal(t, []int{tt.index}, indexes)
			require.NotEmpty(t, tt.msg.String())
			tt.msg.Reset()
			raw, err := proto.Marshal(tt.msg)
			require.NoError(t, err)
			require.Empty(t, raw)
		})
	}

	kind := envelope.MsgType_MSG_TYPE_PUSH
	require.Equal(t, kind, *kind.Enum())
	require.Equal(t, "MSG_TYPE_PUSH", kind.String())
	require.EqualValues(t, 2, kind.Number())
	require.Equal(t, kind.Descriptor(), kind.Type().Descriptor())
	var legacy interface{ EnumDescriptor() ([]byte, []int) } = kind
	data, indexes := legacy.EnumDescriptor()
	require.NotEmpty(t, data)
	require.Equal(t, []int{0}, indexes)
}
