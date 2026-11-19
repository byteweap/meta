package mesh

import (
	"context"
	"time"
)

const defaultDrainTimeout = 10 * time.Second

type drainBudget struct {
	ctx      context.Context
	deadline time.Time
}

// drainErr 同步检查排空期限，避免取消回调尚未得到调度时开始新的执行或回包
func (m *Mesh) drainErr() error {
	budget := m.drain.Load()
	if budget == nil {
		return nil
	}
	if err := budget.ctx.Err(); err != nil {
		return err
	}
	if time.Until(budget.deadline) <= 0 {
		return context.DeadlineExceeded
	}
	return nil
}

// BeginDrain 停止接纳新消息，已入队的工作可在 ctx 的期限内执行和回包
// 无截止时间时使用 10 秒。首次停止确定排空预算。尚未就绪时直接取消启动
func (m *Mesh) BeginDrain(ctx context.Context) {
	m.beginDrain(ctx)
}

func (m *Mesh) beginDrain(ctx context.Context) context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return ctx
	}
	m.stopped = true
	if m.cancel == nil {
		return ctx
	}
	select {
	case <-m.ready:
	default:
		m.cancel()
		return ctx
	}

	cancelRun, cancelReply := m.cancel, m.cancelReply
	abort := func() {
		cancelReply()
		cancelRun()
	}
	if ctx.Err() != nil {
		abort()
		return ctx
	}
	cancelBudget := context.CancelFunc(func() {})
	deadline, ok := ctx.Deadline()
	if !ok {
		ctx, cancelBudget = context.WithTimeout(ctx, defaultDrainTimeout)
		deadline, _ = ctx.Deadline()
	}
	m.drain.Store(&drainBudget{ctx: ctx, deadline: deadline})
	stop := context.AfterFunc(ctx, abort)
	m.releaseDrain = func() {
		stop()
		cancelBudget()
	}
	m.stopInput()
	return ctx
}

// Stop 发起排空并等待清理，调用方可以用新的 ctx 继续等待
// ctx 无期限时，首次调用使用 10 秒排空和等待预算
func (m *Mesh) Stop(ctx context.Context) error {
	ctx = m.beginDrain(ctx)
	m.mu.Lock()
	done, closeErr := m.done, m.stopErr
	m.mu.Unlock()
	if done == nil {
		return closeErr
	}
	select {
	case <-done:
		return m.stopErr
	case <-ctx.Done():
		select {
		case <-done:
			return m.stopErr
		default:
			return ctx.Err()
		}
	}
}
