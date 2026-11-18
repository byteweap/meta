package gate

import (
	"context"
	"errors"
	"net/http"
	"runtime/debug"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/pkg/async"
	"github.com/byteweap/meta/pkg/conv"
)

// handlerRequestReplyMessage 来自其它服务的(request-reply)消息
func (g *Gate) handleRequestReplyMessage(msg *broker.Message) {
	if msg == nil {
		return
	}
	if err := g.replyError(msg, http.StatusNotImplemented, "gate request-reply is not implemented"); err != nil {
		log.Errorf("[websocket] request-reply unsupported, reply error: %v", err)
	}
}

// handlerPubSubMessage 来自Mesh服务的(pub-sub)消息
func (g *Gate) handlePubSubMessage(msg *broker.Message) {
	uid := cluster.GetUIDBy(msg.Header)
	if uid <= 0 {
		log.Errorf("[websocket] reply2player get uid error, uid: %v", uid)
		return
	}
	session, ok := g.sessions.get(uid)
	if !ok {
		log.Errorf("[websocket] reply2player get session error, uid: %v", uid)
		return
	}
	delivery := msg.Header.Get(cluster.FieldNameDelivery)
	sessionID := cluster.GetSessionIDBy(msg.Header)
	switch delivery {
	case cluster.DeliveryResponse, "":
		if g.opts.strictSessionDelivery && (delivery == "" || sessionID == "") {
			log.Warnf("[websocket] missing response delivery metadata, uid: %v", uid)
			return
		}
		if sessionID != "" {
			currentID, _ := session.Get(cluster.FieldNameSessionID)
			if currentID != sessionID {
				log.Debugf("[websocket] dropping response to a stale session, uid: %v", uid)
				return
			}
		}
	case cluster.DeliveryPush:
		if sessionID != "" {
			log.Warnf("[websocket] push contains a session ID, uid: %v", uid)
			return
		}
	default:
		log.Warnf("[websocket] unknown delivery mode, uid: %v", uid)
		return
	}
	if err := session.WriteBinary(msg.Data); err != nil {
		log.Errorf("[websocket] reply2player write binary error, uid: %v, err: %v", uid, err)
		return
	}
	log.Debugf("[websocket] reply2player success, uid: %v", uid)
}

// 处理来自其它服务的消息
func (g *Gate) handleMessage(msg *broker.Message) {
	defer async.Recover(func(r any) {
		log.Errorf("gate message handler panic: %v\n%s", r, debug.Stack())
	})
	if msg.Reply != "" {
		g.handleRequestReplyMessage(msg)
	} else {
		g.handlePubSubMessage(msg)
	}
}

func (g *Gate) replyError(reqMsg *broker.Message, code int, tip string) error {
	if reqMsg == nil {
		return errors.New("request message is nil")
	}
	if reqMsg.Reply == "" {
		return errors.New("reply subject is empty")
	}
	header := cloneHeader(reqMsg.Header)
	header.Set("code", conv.String(code))
	header.Set("tip", tip)
	return g.opts.broker.Reply(g.ctx, reqMsg, nil, broker.ReplyHeader(header))
}

// 将已校验的业务消息原样分发至 Mesh
func (g *Gate) dispatch(uid int64, sessionID, toService string, data []byte) {
	if toService == "" {
		log.Errorf("[websocket] dispatch error, uid: %v, service is empty", uid)
		return
	}

	curNodeID, err := g.opts.locator.Node(g.ctx, uid, toService)
	if err != nil {
		log.Errorf("[websocket] dispatch | get mesh node error, uid: %v, toService: %v, err: %v", uid, toService, err)
		return
	}

	nodeID := curNodeID
	if curNodeID == "" {
		sel, ensureErr := g.ensure(toService)
		if ensureErr != nil {
			log.Errorf("[websocket] dispatch | get mesh node error, uid: %v, toService: %v, err: %v", uid, toService, ensureErr)
			return
		}
		node, selectErr := sel.Select(conv.String(uid))
		if selectErr != nil {
			log.Errorf("[websocket] dispatch | select mesh node error, uid: %v, toService: %v, err: %v", uid, toService, selectErr)
			return
		}
		nodeID = node.ID()
	}
	// 构建消息头
	var (
		reply  = g.Subject(toService) // 回复主题
		header = cluster.BuildHeader(uid, cluster.EventBusiness, reply, g.appName, toService)
	)
	header.Set(cluster.FieldNameSessionID, sessionID)
	// 发布消息到 Mesh
	subject := cluster.Subject(g.opts.prefix, g.appName, toService, nodeID)
	if err := g.opts.broker.Pub(g.ctx, subject, data, broker.PubHeader(header)); err != nil {
		log.Errorf("[websocket] dispatch error, uid: %v, subject: %v, err: %v", uid, subject, err)
		return
	}
	log.Debugf("[websocket] dispatch success, uid: %v, subject: %v", uid, subject)
}

// 广播系统事件
func (g *Gate) broadcastEvent(ctx context.Context, uid int64, sessionID string, event cluster.Event) error {
	// 获取玩家当前所在所有节点
	snMap, err := g.opts.locator.AllNodes(ctx, uid)
	if err != nil {
		log.Errorf("[websocket] broadcast event, get all nodes error, uid: %v, event: %v, err: %v", uid, event, err)
		return err
	}
	var result error
	for service, node := range snMap {
		if service == g.appName { // 不包括 gate
			continue
		}
		// 发布消息到 Mesh
		var (
			header  = cluster.BuildHeader(uid, event, g.Subject(service), g.appName, service)
			subject = cluster.Subject(g.opts.prefix, g.appName, service, node)
		)
		header.Set(cluster.FieldNameSessionID, sessionID)
		if err = g.opts.broker.Pub(ctx, subject, nil, broker.PubHeader(header)); err != nil {
			result = errors.Join(result, err)
			log.Errorf("[websocket] broadcast event error, uid: %v, subject: %v, err: %v", uid, subject, err)
			continue
		}
		log.Debugf("[websocket] broadcast event success, uid: %v, subject: %v, event: %v", uid, subject, event)
	}
	return result
}

func cloneHeader(src broker.Header) broker.Header {
	dst := broker.Header{}
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
	return dst
}
