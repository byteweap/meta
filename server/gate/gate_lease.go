package gate

import (
	"context"
	"errors"
	"time"

	"github.com/byteweap/meta/component/log"
)

const (
	gateLeaseTTL      = time.Minute
	gateLeaseInterval = 15 * time.Second
)

var errGateLeaseLost = errors.New("gate: run lease expired")

func (g *Gate) acquireGateLease(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, gateLeaseInterval)
	defer cancel()
	return g.opts.locator.AcquireGateLease(ctx, g.runID, gateLeaseTTL)
}

// maintainGateLease 返回 true 表示存储拒绝续期或最后一次确认的有效期已结束
func (g *Gate) maintainGateLease(ctx context.Context, validUntil time.Time) bool {
	timer := time.NewTimer(min(gateLeaseInterval, time.Until(validUntil)))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return false
		}
		started := time.Now()
		remaining := validUntil.Sub(started)
		if remaining <= 0 {
			return true
		}
		renewCtx, cancel := context.WithDeadline(ctx, started.Add(min(gateLeaseInterval, remaining)))
		renewed, err := g.opts.locator.RenewGateLease(renewCtx, g.runID, gateLeaseTTL)
		cancel()
		if ctx.Err() != nil {
			return false
		}
		switch {
		case err != nil:
			log.Errorf("renew Gate lease: %v", err)
		case !renewed:
			return true
		default:
			// 用请求开始时间作为下界，存储实际续期只会发生得更晚
			validUntil = started.Add(gateLeaseTTL)
		}
		timer.Reset(min(time.Until(started.Add(gateLeaseInterval)), time.Until(validUntil)))
	}
}

func (g *Gate) stopAfterLeaseLoss(ctx context.Context) {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), disconnectTimeout)
	defer cancel()
	if err := g.Stop(stopCtx); err != nil {
		log.Errorf("stop Gate after lease loss: %v", err)
	}
}
