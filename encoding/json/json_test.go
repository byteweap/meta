package json

import (
	stdjson "encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/byteweap/meta/encoding"
)

func TestMarshalAndUnmarshalJSON(t *testing.T) {
	codec := encoding.GetCodec(Name)
	require.NotNil(t, codec)
	require.Equal(t, Name, codec.Name())

	type record struct{ Value string }
	want := record{Value: "雪"}
	data, err := Marshal(want)
	require.NoError(t, err)
	require.JSONEq(t, `{"Value":"雪"}`, string(data))

	var allocated *record
	require.NoError(t, Unmarshal(data, &allocated))
	require.Equal(t, &want, allocated)
	require.Error(t, Unmarshal([]byte("{"), &allocated))

	raw := stdjson.RawMessage(`{"raw":true}`)
	data, err = Marshal(raw)
	require.NoError(t, err)
	require.Equal(t, []byte(raw), data)
	var decoded stdjson.RawMessage
	require.NoError(t, Unmarshal(data, &decoded))
	require.Equal(t, raw, decoded)
}

func TestProtobufJSON(t *testing.T) {
	data, err := Marshal(wrapperspb.String("snow"))
	require.NoError(t, err)
	require.Equal(t, `"snow"`, string(data))

	direct := new(wrapperspb.StringValue)
	require.NoError(t, Unmarshal(data, direct))
	require.Equal(t, "snow", direct.GetValue())

	wrapped := any(new(wrapperspb.StringValue))
	require.NoError(t, Unmarshal(data, &wrapped))
	require.Equal(t, "snow", wrapped.(*wrapperspb.StringValue).GetValue())
}
