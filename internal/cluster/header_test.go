package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/broker"
)

func TestHeaderRoundTrip(t *testing.T) {
	header := BuildHeader(42, EventReconnect, "reply", "gate", "game")
	header.Set(FieldNameSessionID, "session-1")
	require.EqualValues(t, 42, GetUIDBy(header))
	require.Equal(t, EventReconnect, GetEventBy(header))
	require.Equal(t, "reply", GetReplyBy(header))
	require.Equal(t, "gate", GetFromServiceBy(header))
	require.Equal(t, "game", GetToServiceBy(header))
	require.Equal(t, "session-1", GetSessionIDBy(header))

	require.Empty(t, GetSessionIDBy(nil))
	require.Zero(t, GetUIDBy(broker.Header{FieldNameUID: []string{"bad-id"}}))
}

func TestSubjectWithAndWithoutPrefix(t *testing.T) {
	require.Equal(t, "meta.gate.game.node-1", Subject("meta", "gate", "game", "node-1"))
	require.Equal(t, "gate.game.node-1", Subject("", "gate", "game", "node-1"))
}
