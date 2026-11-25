package meta

import (
	"bytes"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/log"
)

func TestAppReportsConfiguredIdentityAndEndpoint(t *testing.T) {
	previous := log.GetLogger()
	t.Cleanup(func() { log.SetLogger(previous) })
	configuredLogger := log.NewStdLogger(new(bytes.Buffer))
	configuredEndpoint := &url.URL{Scheme: "mesh", Host: "game.local:9000"}
	deadlines := []time.Duration{}
	registry := &lifecycleRegistry{
		register: func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if ok {
				deadlines = append(deadlines, time.Until(deadline))
			}
			return nil
		},
	}
	a := New(ID("node"), Name("game"), Version("v2"), Metadata(map[string]string{"zone": "test"}),
		Endpoint(configuredEndpoint), Logger(configuredLogger), Registry(registry), RegistryTimeout(time.Second))
	t.Cleanup(func() { _ = a.Stop() })
	require.Equal(t, "node", a.ID())
	require.Equal(t, "game", a.Name())
	require.Equal(t, "v2", a.Version())
	require.Equal(t, map[string]string{"zone": "test"}, a.Metadata())
	require.Same(t, configuredLogger, log.GetLogger())
	require.Empty(t, a.Endpoint())
	require.NoError(t, a.buildInstance(t.Context()))
	require.Equal(t, []string{configuredEndpoint.String()}, a.Endpoint())
	require.NoError(t, a.register(t.Context()))
	require.Len(t, deadlines, 1)
	require.Positive(t, deadlines[0])
	require.LessOrEqual(t, deadlines[0], time.Second)
}
