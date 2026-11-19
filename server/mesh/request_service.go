package mesh

import (
	"context"
	"fmt"
	"strings"

	es "github.com/byteweap/meta/errors"
)

// RequestService 按服务名选择一个实例发送 RPC，返回值与 Request 相同
// 在 Mesh 启动并配置 Discovery、SelectorFunc 后可用，ctx 的期限覆盖发现与回包
// 没有可用实例时返回 selector.ErrNoAvailableNode，发送失败后不会重试
func (m *Mesh) RequestService(ctx context.Context, service, cmd, version string, data []byte) ([]byte, string, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", 0, err
	}
	m.mu.Lock()
	r, from, runCtx := m.resolver, m.appName, m.ctx
	m.mu.Unlock()
	if err := runCtx.Err(); err != nil {
		return nil, "", 0, err
	}
	if r == nil {
		if m.opts.discovery == nil {
			return nil, "", 0, es.ErrDiscoveryRequired
		}
		if m.opts.selectorFunc == nil {
			return nil, "", 0, es.ErrSelectorRequired
		}
		return nil, "", 0, es.ErrAppNotFound
	}
	if !validSubjectToken(from) || !validSubjectToken(service) {
		return nil, "", 0, fmt.Errorf("invalid RPC service name: %q -> %q", from, service)
	}
	s, err := r.Resolve(ctx, service)
	if err != nil {
		return nil, "", 0, err
	}
	subject, err := s.SelectSubject("")
	if err != nil {
		return nil, "", 0, err
	}
	return m.Request(ctx, subject, cmd, version, data)
}

func validSubjectToken(s string) bool {
	return s != "" && !strings.ContainsAny(s, ".*/\\> \t\r\n\x00")
}
