package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/vo"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/contrib/broker/nats"
)

func TestDemoUserIDExtractor(t *testing.T) {
	resolve := demoUserIDExtractor("local-test-token")
	for header, want := range map[string]int64{"": 0, "Bearer wrong": 0, "Bearer local-test-token": 1} {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header.Set("Authorization", header)
		if userID := resolve(request); userID != want {
			t.Fatalf("userID = %d, want %d", userID, want)
		}
	}
}

func TestRunGateBuildsAppAndLogsRunError(t *testing.T) {
	t.Setenv("GATE_DEMO_TOKEN", "local-test-token")
	var output bytes.Buffer
	previous := log.GetLogger()
	log.SetLogger(log.NewStdLogger(&output))
	t.Cleanup(func() { log.SetLogger(previous) })
	run(func(params vo.NacosClientParam) (naming_client.INamingClient, error) {
		if len(params.ServerConfigs) != 1 || !params.ClientConfig.UpdateCacheWhenEmpty {
			t.Fatalf("unexpected Nacos configuration: %+v", params)
		}
		return nil, nil
	}, func(...nats.Option) (*nats.Broker, error) { return nats.NewWith(nil), nil }, func(app *meta.App) error {
		if app.Name() != "gate" || app.Version() != "v1.0.0" || !strings.HasPrefix(app.ID(), "gate-") {
			t.Fatalf("unexpected application identity: %s %s %s", app.Name(), app.Version(), app.ID())
		}
		return errors.New("gate server failed")
	})
	if !strings.Contains(output.String(), "gate server failed") {
		t.Fatalf("run error missing from log: %s", &output)
	}
}

func TestRunGateRejectsMissingTokenAndStartupFailures(t *testing.T) {
	failure := errors.New("startup failed")
	for _, test := range []struct {
		name, token string
		connect     func(vo.NacosClientParam) (naming_client.INamingClient, error)
		broker      func(...nats.Option) (*nats.Broker, error)
		want        any
	}{
		{name: "token", want: "GATE_DEMO_TOKEN is required"},
		{name: "naming", token: "local-test-token", connect: func(vo.NacosClientParam) (naming_client.INamingClient, error) {
			return nil, failure
		}, want: failure},
		{name: "broker", token: "local-test-token", connect: func(vo.NacosClientParam) (naming_client.INamingClient, error) {
			return nil, nil
		}, broker: func(...nats.Option) (*nats.Broker, error) { return nil, failure }, want: failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("GATE_DEMO_TOKEN", test.token)
			defer func() {
				if got := recover(); got != test.want {
					t.Fatalf("startup panic = %v, want %v", got, test.want)
				}
			}()
			run(test.connect, test.broker, nil)
		})
	}
}
