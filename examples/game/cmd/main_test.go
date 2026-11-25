package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/vo"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/examples/game/internal/server"
)

func TestRunGameBuildsAppAndCleansUpOnRunFailure(t *testing.T) {
	var output bytes.Buffer
	previous := log.GetLogger()
	log.SetLogger(log.NewStdLogger(&output))
	t.Cleanup(func() { log.SetLogger(previous) })
	cleaned := false
	run(func(params vo.NacosClientParam) (naming_client.INamingClient, error) {
		if len(params.ServerConfigs) != 1 || params.ServerConfigs[0].ContextPath != "/nacos" || !params.ClientConfig.UpdateCacheWhenEmpty {
			t.Fatalf("unexpected Nacos configuration: %+v", params)
		}
		return nil, nil
	}, func() (*server.Server, func(), error) {
		return server.New(), func() { cleaned = true }, nil
	}, func(app *meta.App) error {
		if app.Name() != "game" || app.Version() != "v1.0.0" || !strings.HasPrefix(app.ID(), "game-") {
			t.Fatalf("unexpected application identity: %s %s %s", app.Name(), app.Version(), app.ID())
		}
		return errors.New("game server failed")
	})
	if !cleaned || !strings.Contains(output.String(), "game server failed") {
		t.Fatalf("cleanup=%v, log=%q", cleaned, output.String())
	}
}

func TestRunGamePropagatesStartupFailure(t *testing.T) {
	failure := errors.New("startup failed")
	t.Run("naming", func(t *testing.T) {
		defer func() {
			if got := recover(); got != failure {
				t.Fatalf("startup panic = %v, want %v", got, failure)
			}
		}()
		run(func(vo.NacosClientParam) (naming_client.INamingClient, error) { return nil, failure }, nil, nil)
	})
	t.Run("game", func(t *testing.T) {
		defer func() {
			if got := recover(); got != failure {
				t.Fatalf("startup panic = %v, want %v", got, failure)
			}
		}()
		run(func(vo.NacosClientParam) (naming_client.INamingClient, error) { return nil, nil },
			func() (*server.Server, func(), error) { return nil, nil, failure }, nil)
	})
}
