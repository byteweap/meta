package gate

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

func statelessGameInstance(id string) *registry.ServiceInstance {
	instance := discoveryTestInstance("game", id)
	instance.Metadata["stateless"] = "true"
	return instance
}

type statelessTestSelector struct {
	testSelector
	key  string
	fail bool
}

func (s *statelessTestSelector) Select(key string, filters ...selector.Filter) (selector.Node, error) {
	s.key = key
	if s.fail || key == "" {
		return nil, selector.ErrNoAvailableNode
	}
	return s.testSelector.Select(key, filters...)
}

func TestGateStatelessRoutingFollowsDiscovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newInitialDiscoveryWatcher()
		first, second := statelessGameInstance("game-1"), statelessGameInstance("game-2")
		initial := []*registry.ServiceInstance{first, second}
		instances := initial
		reg := &initialDiscoveryRegistry{
			watch: func(context.Context, string) (registry.Watcher, error) { return w, nil },
			get:   func(context.Context, string) ([]*registry.ServiceInstance, error) { return instances, nil },
		}
		bound := "game-2"
		loc := &dispatchTestLocator{node: func() (string, error) { return bound, nil }}
		bro := &testBroker{}
		sel := &statelessTestSelector{}
		g := New(Discovery(reg), Locator(loc), Broker(bro), SelectorFunc(func() selector.Selector { return sel }))
		g.appName, g.appID = "gate", "gate-1"
		t.Cleanup(func() { require.NoError(t, g.Stop(context.Background())) })
		dispatch := func() error { return g.dispatch(42, "session-42", "game", nil) }

		require.NoError(t, dispatch())
		require.Equal(t, "42", sel.key)
		require.Zero(t, loc.calls.Load())
		sel.fail = true
		require.ErrorIs(t, dispatch(), selector.ErrNoAvailableNode)
		require.Zero(t, loc.calls.Load(), "选择器失败不能回退到玩家定位")
		sel.fail = false

		w.send(t, []*registry.ServiceInstance{first, discoveryTestInstance("game", "game-2")})
		synctest.Wait()
		require.NoError(t, dispatch())
		bound = "offline"
		require.ErrorIs(t, dispatch(), selector.ErrNoAvailableNode)
		bound = ""
		require.NoError(t, dispatch())
		require.Equal(t, "42", sel.key)
		require.EqualValues(t, 3, loc.calls.Load())

		w.send(t, initial)
		synctest.Wait()
		require.NoError(t, dispatch())
		require.Equal(t, "42", sel.key)
		require.EqualValues(t, 3, loc.calls.Load())
		require.EqualValues(t, 2, reg.getCount.Load())
		require.EqualValues(t, 1, reg.watchCount.Load())
		require.Equal(t, []string{
			"meta.gate.game.game-1", "meta.gate.game.game-2", "meta.gate.game.game-1", "meta.gate.game.game-1",
		}, bro.pubSubjects)

		instances = nil
		w.send(t, nil)
		synctest.Wait()
		require.ErrorIs(t, dispatch(), errServiceNotFound)
		require.EqualValues(t, 3, loc.calls.Load())
	})
}
