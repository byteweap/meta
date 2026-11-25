package mesh

import (
	"context"
	"net/http"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/pkg/async"
)

const (
	clientOverloadReplyTimeout = 100 * time.Millisecond

	// 字段编号由 idl/envelope/v1/envelope.proto 中的 ClientEnvelope 定义
	clientEnvelopeHeaderField  = 1
	clientEnvelopeServiceField = 2
)

func (m *Mesh) rejectClientRequest(inputCtx context.Context, msg *broker.Message, canReply bool) {
	if inputCtx.Err() != nil {
		m.reject(observe.Stopping)
		return
	}
	m.reject(observe.QueueFull)
	if !canReply {
		return
	}
	defer async.Recover(logMessagePanic)
	if cluster.GetUIDBy(msg.Header) <= 0 || cluster.GetReplyBy(msg.Header) == "" || cluster.GetSessionIDBy(msg.Header) == "" {
		return
	}
	var header envelope.Header
	if !decodeClientReplyHeader(msg.Data, &header) {
		return
	}

	ctx, cancel := context.WithTimeout(inputCtx, clientOverloadReplyTimeout)
	defer cancel()
	response := newContext(m, msg, &envelope.ClientEnvelope{Header: &header})
	defer response.release()
	response.respondWithContext(ctx, nil, &envelope.ResponseError{
		Code: http.StatusServiceUnavailable, Message: http.StatusText(http.StatusServiceUnavailable),
	})
}

// decodeClientReplyHeader 校验完整的外层帧，合并重复请求头并跳过载荷拷贝
func decodeClientReplyHeader(data []byte, header *envelope.Header) bool {
	decode := proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}
	found := false
	for len(data) > 0 {
		field, typ, n := protowire.ConsumeTag(data)
		if n < 0 || !field.IsValid() {
			return false
		}
		data = data[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(field, typ, data)
			if n < 0 {
				return false
			}
			data = data[n:]
			continue
		}
		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return false
		}
		switch field {
		case clientEnvelopeHeaderField:
			if err := decode.Unmarshal(value, header); err != nil {
				return false
			}
			found = true
		case clientEnvelopeServiceField:
			if !utf8.Valid(value) {
				return false
			}
		}
		data = data[n:]
	}
	return found
}
