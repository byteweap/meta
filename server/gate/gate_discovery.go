package gate

import "github.com/byteweap/meta/internal/discovery"

var errServiceNotFound = discovery.ErrNotFound

func (g *Gate) ensure(service string) (*discovery.Service, error) {
	return g.resolver().Resolve(g.ctx, service)
}
