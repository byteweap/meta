package internal

import (
	"errors"
	"testing"

	"github.com/byteweap/meta/contrib/broker/nats"
)

func TestNewApplicationBrokerFailure(t *testing.T) {
	failure := errors.New("broker unavailable")
	app, closeApplication, err := newApplication(func(...nats.Option) (*nats.Broker, error) { return nil, failure })
	if app != nil || closeApplication != nil || !errors.Is(err, failure) {
		t.Fatalf("application=%v cleanup present=%v error=%v", app, closeApplication != nil, err)
	}
}

func TestNewApplicationBuildsGameServer(t *testing.T) {
	app, closeApplication, err := newApplication(func(...nats.Option) (*nats.Broker, error) { return nats.NewWith(nil), nil })
	if err != nil || app == nil || closeApplication == nil || app.NumPlayers() != 0 || app.NumRooms() != 0 {
		t.Fatalf("app=%v cleanup present=%v error=%v", app, closeApplication != nil, err)
	}
	closeApplication()
}
