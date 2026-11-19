package mesh

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/pkg/async"
)

const clientOverloadReplyTimeout = 100 * time.Millisecond

func (m *Mesh) rejectClientRequest(inputCtx context.Context, msg *broker.Message) {
	defer async.Recover(logMessagePanic)
	if inputCtx.Err() != nil {
		m.reject(observe.Stopping)
		return
	}
	m.reject(observe.QueueFull)
	if cluster.GetUIDBy(msg.Header) <= 0 || cluster.GetReplyBy(msg.Header) == "" || cluster.GetSessionIDBy(msg.Header) == "" {
		return
	}
	var frame envelope.ClientEnvelope
	if err := proto.Unmarshal(msg.Data, &frame); err != nil || frame.GetHeader() == nil {
		return
	}

	ctx, cancel := context.WithTimeout(inputCtx, clientOverloadReplyTimeout)
	defer cancel()
	response := newContext(m, msg, &frame)
	defer response.release()
	response.respondWithContext(ctx, nil, &envelope.ResponseError{
		Code: http.StatusServiceUnavailable, Message: http.StatusText(http.StatusServiceUnavailable),
	})
}
