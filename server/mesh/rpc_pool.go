package mesh

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/pkg/async"
)

type rpcPool struct {
	mesh     *Mesh
	jobs     chan queuedMessage
	inFlight sync.WaitGroup // worker 和正在回复过载的回调
	mu       sync.Mutex
	free     int // 可用的执行和排队名额，直至回包完成才归还
	closed   bool
}

func newRPCPool(m *Mesh, capacity int) *rpcPool {
	return &rpcPool{mesh: m, jobs: make(chan queuedMessage, capacity), free: capacity}
}

func (p *rpcPool) receiver(inputCtx context.Context, next broker.Handler) broker.Handler {
	return func(msg *broker.Message) {
		if msg == nil || msg.Reply == "" {
			next(msg)
			return
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			p.mesh.reject(observe.Stopping)
			return
		}
		if p.free > 0 && inputCtx.Err() == nil {
			// 预留名额保证投递不会阻塞，即使空闲 worker 尚未得到调度
			p.free--
			q := queuedMessage{msg: msg}
			if p.mesh.opts.observer != nil {
				q.at = time.Now()
			}
			p.jobs <- q
			p.mu.Unlock()
			return
		}
		reason := observe.QueueFull
		if p.mesh.opts.observer != nil && inputCtx.Err() != nil {
			reason = observe.Stopping
		}
		p.inFlight.Add(1)
		p.mu.Unlock()
		defer p.inFlight.Done()
		var replies rpcReplyTimer
		defer replies.stop()
		defer async.Recover(logMessagePanic)
		p.mesh.reject(reason)
		p.mesh.replyRequestResult(&replies, msg, nil, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
	}
}

func (p *rpcPool) start(workers int) {
	for range workers {
		p.inFlight.Go(func() {
			var replies rpcReplyTimer
			defer replies.stop()
			for q := range p.jobs {
				p.handle(q, &replies)
				p.mu.Lock()
				p.free++
				p.mu.Unlock()
			}
		})
	}
}

func (p *rpcPool) close() {
	p.mu.Lock()
	p.closed = true
	close(p.jobs)
	p.mu.Unlock()

	if p.mesh.ctx.Err() != nil {
		// 启动失败或硬取消时不能依赖 worker，队列里的 RPC 直接按停服规则处理
		var replies rpcReplyTimer
		defer replies.stop()
		for q := range p.jobs {
			p.handle(q, &replies)
		}
	}
	p.inFlight.Wait()
}

func (p *rpcPool) handle(q queuedMessage, replies *rpcReplyTimer) {
	if !q.at.IsZero() {
		p.mesh.opts.observer.Observe(observe.Queue, time.Since(q.at))
	}
	p.mesh.handleRPC(q.msg, replies)
}

// rpcReplyTimer 由单个执行协程持有，为连续回包复用独立于业务取消信号的发送期限
type rpcReplyTimer struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (r *rpcReplyTimer) context(parent context.Context) context.Context {
	if r.ctx != nil {
		deadline, _ := r.ctx.Deadline()
		if time.Until(deadline) >= rpcReplyTimeout/2 {
			return r.ctx
		}
		r.cancel()
	}
	r.ctx, r.cancel = context.WithTimeout(parent, rpcReplyTimeout)
	return r.ctx
}

func (r *rpcReplyTimer) stop() {
	if r.cancel != nil {
		r.cancel()
	}
}
