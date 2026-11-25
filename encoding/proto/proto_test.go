package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/byteweap/meta/encoding"
)

func TestWireRoundTrip(t *testing.T) {
	codec := encoding.GetCodec(Name)
	require.NotNil(t, codec)
	require.Equal(t, Name, codec.Name())

	data, err := Marshal(wrapperspb.Int64(42))
	require.NoError(t, err)
	require.Equal(t, []byte{8, 42}, data)

	direct := new(wrapperspb.Int64Value)
	require.NoError(t, Unmarshal(data, direct))
	require.EqualValues(t, 42, direct.GetValue())

	indirect := new(wrapperspb.Int64Value)
	require.NoError(t, Unmarshal(data, &indirect))
	require.EqualValues(t, 42, indirect.GetValue())
	require.EqualError(t, Unmarshal(data, &struct{}{}), "not proto message")
	require.Error(t, Unmarshal([]byte{0xff}, direct))
}
