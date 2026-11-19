// Package observation 隔离请求观测回调中的 panic
package observation

import (
	"runtime/debug"
	"time"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/pkg/async"
)

// Protect 保留 nil 观测器，并隔离其他观测器回调中的 panic
func Protect(observer observe.Observer) observe.Observer {
	if observer == nil {
		return nil
	}
	return protectedObserver{observer: observer}
}

type protectedObserver struct {
	observer observe.Observer
}

func (o protectedObserver) Observe(stage observe.Stage, elapsed time.Duration) {
	defer async.Recover(logPanic)
	o.observer.Observe(stage, elapsed)
}

func (o protectedObserver) Reject(reason observe.Reason) {
	defer async.Recover(logPanic)
	o.observer.Reject(reason)
}

func logPanic(r any) {
	log.Errorf("request observer panic: %v\n%s", r, debug.Stack())
}
