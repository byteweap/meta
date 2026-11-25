package yaml

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/encoding"
)

func TestRoundTrip(t *testing.T) {
	type record struct{ Value string }
	want := record{Value: "雪"}
	codec := encoding.GetCodec(Name)
	require.NotNil(t, codec)
	require.Equal(t, Name, codec.Name())

	data, err := Marshal(want)
	require.NoError(t, err)
	var got record
	require.NoError(t, Unmarshal(data, &got))
	require.Equal(t, want, got)
	require.Error(t, Unmarshal([]byte("["), &got))
}
