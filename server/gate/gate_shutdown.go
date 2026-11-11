package gate

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/olahol/melody"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/registry"
)

const disconnectTimeout = 10 * time.Second

// beginTask 与停止标记共用锁，确保停止后不会向等待组添加新任务。
func (g *Gate) beginTask() bool {
	g.lifecycleMu.Lock()
	defer g.lifecycleMu.Unlock()
	if g.stopped || g.ctx.Err() != nil {
		return false
	}
	g.tasks.Add(1)
	return true
}

// Stop 发起一次清理，并在调用方的期限内等待真实完成。
func (g *Gate) Stop(ctx context.Context) error {
	g.lifecycleMu.Lock()
	if !g.stopped {
		g.stopped = true
		g.stopCtx = ctx
		g.cancel()
		go g.shutdown(ctx, g.ln, g.ws)
	}
	g.lifecycleMu.Unlock()

	select {
	case <-g.stopDone:
		return g.stopError()
	default:
	}
	select {
	case <-g.stopDone:
		return g.stopError()
	case <-ctx.Done():
		return errors.Join(g.stopError(), ctx.Err())
	}
}

func (g *Gate) shutdown(ctx context.Context, ln net.Listener, ws *melody.Melody) {
	var closing sync.WaitGroup
	closeResource := func(close func() error) {
		closing.Go(func() { g.addStopError(close()) })
	}
	closeResource(func() error {
		err := g.Shutdown(ctx)
		if err != nil {
			err = errors.Join(err, g.Close())
		}
		// 先标记 HTTP 服务器已停止，再关闭尚未 Serve 的监听器。
		if ln != nil {
			err = errors.Join(err, ignoreClosed(ln.Close()))
		}
		return err
	})

	// Melody.Close 不等待断开，且可能漏掉升级中的连接。
	// 先关闭现有连接；迟到连接由 handleConnect 拒绝。
	if ws != nil {
		sessions, err := ws.Sessions()
		if err != nil && !errors.Is(err, melody.ErrClosed) {
			g.addStopError(err)
		}
		for _, session := range sessions {
			closeResource(func() error { return ignoreClosed(session.WebsocketConnection().Close()) })
		}
	}

	g.mu.Lock()
	watchers := make([]registry.Watcher, 0, len(g.watchers))
	for _, watcher := range g.watchers {
		watchers = append(watchers, watcher)
	}
	clear(g.watchers)
	clear(g.selectors)
	g.mu.Unlock()
	for _, watcher := range watchers {
		closeResource(watcher.Stop)
	}

	g.tasks.Wait()
	closing.Wait()
	if ws != nil {
		if err := ws.Close(); err != nil && !errors.Is(err, melody.ErrClosed) {
			g.addStopError(err)
		}
	}
	close(g.stopDone)
	log.Info("[gate] server stopped")
}

func ignoreClosed(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (g *Gate) addStopError(err error) {
	if err == nil {
		return
	}
	g.lifecycleMu.Lock()
	g.stopErr = errors.Join(g.stopErr, err)
	g.lifecycleMu.Unlock()
}

func (g *Gate) stopError() error {
	g.lifecycleMu.Lock()
	defer g.lifecycleMu.Unlock()
	return g.stopErr
}

func (g *Gate) disconnectContext() (context.Context, context.CancelFunc) {
	g.lifecycleMu.Lock()
	ctx := g.stopCtx
	if ctx == nil {
		ctx = context.WithoutCancel(g.ctx)
	}
	g.lifecycleMu.Unlock()
	return context.WithTimeout(ctx, disconnectTimeout)
}
