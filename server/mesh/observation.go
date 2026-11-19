package mesh

import (
	"time"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/internal/cluster"
)

type queuedMessage struct {
	msg *broker.Message
	at  time.Time // 零值表示未配置观测或消息是会话事件
}

func (m *Mesh) queued(msg *broker.Message) queuedMessage {
	q := queuedMessage{msg: msg}
	if m.opts.observer != nil && isRequest(msg) {
		q.at = time.Now()
	}
	return q
}

func (m *Mesh) reject(reason observe.Reason) {
	if observer := m.opts.observer; observer != nil {
		observer.Reject(reason)
	}
}

func isRequest(msg *broker.Message) bool {
	if msg == nil {
		return false
	}
	if msg.Reply != "" {
		return true
	}
	switch cluster.GetEventBy(msg.Header) {
	case cluster.EventOnline, cluster.EventOffline, cluster.EventReconnect:
		return false
	default:
		return true
	}
}
