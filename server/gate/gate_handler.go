package gate

import (
	"crypto/rand"
	"errors"

	"github.com/olahol/melody"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/encoding/proto"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

// 连接建立时调用
func (g *Gate) handleConnect(s *melody.Session) {
	value, _ := s.Get("uid")
	uid := value.(int64)
	unlock := g.sessions.lock(uid)
	defer unlock()

	g.lifecycleMu.Lock()
	stopping := g.stopped || g.ctx.Err() != nil
	g.lifecycleMu.Unlock()
	if stopping {
		_ = s.WebsocketConnection().Close()
		return
	}
	// 绑定成功后再切换连接，失败的新连接不能破坏原会话。
	sessionID := rand.Text()
	replaced, err := g.opts.locator.Claim(g.ctx, uid, g.appName, g.appID, sessionID)
	if err != nil {
		log.Errorf("[websocket] new connection bind gate error, uid: %v, err: %v", uid, err)
		_ = s.WebsocketConnection().Close()
		return
	}
	s.Set(cluster.FieldNameSessionID, sessionID)
	previous, localReplaced := g.sessions.replace(uid, s)
	if localReplaced {
		_ = previous.WebsocketConnection().Close()
	}
	log.Infof("[websocket] new connection success, uid: %v, %s", uid, sessionRemoteAddr(s))

	// 广播 上线、重连 事件到上游服务
	event := cluster.EventOnline
	if replaced {
		event = cluster.EventReconnect
	}
	_ = g.broadcastEvent(g.ctx, uid, sessionID, event)
}

// 连接断开时调用
func (g *Gate) handleDisconnect(s *melody.Session) {
	uids, ok := s.Get("uid")
	if !ok {
		log.Error("[websocket] connection disconnect error, session not contains uid key")
		return
	}
	uid := uids.(int64)
	unlock := g.sessions.lock(uid)
	defer unlock()

	// 注销会话
	if !g.sessions.unregisterIfSame(uid, s) {
		log.Warnf("[websocket] connection disconnect error, uid: %v session not match", uid)
		return
	}

	log.Infof("[websocket] connection disconnect success, uid: %v", uid)
	ctx, cancel := g.disconnectContext()
	defer cancel()

	// 解绑网关
	value, _ := s.Get(cluster.FieldNameSessionID)
	sessionID, _ := value.(string)
	released, err := g.opts.locator.Release(ctx, uid, g.appName, g.appID, sessionID)
	if err != nil {
		log.Errorf("[websocket] connection disconnect success, unbind gate error, uid: %v, err: %v", uid, err)
	} else if released {
		err = g.broadcastEvent(ctx, uid, sessionID, cluster.EventOffline)
	}

	g.lifecycleMu.Lock()
	if g.stopped {
		g.stopErr = errors.Join(g.stopErr, err)
	}
	g.lifecycleMu.Unlock()
}

// 接收到文本消息时调用
func (g *Gate) handleTextMessage(_ *melody.Session, _ []byte) {
	// todo
}

// 接收到二进制消息时调用
func (g *Gate) handleBinaryMessage(s *melody.Session, msg []byte) {
	var incoming envelope.ClientEnvelope
	if err := proto.Unmarshal(msg, &incoming); err != nil {
		log.Errorf("[websocket] unmarshal envelope error: %v", err)
		return
	}
	if incoming.GetHeader() == nil {
		log.Error("[websocket] envelope header is required")
		return
	}
	uids, ok := s.Get("uid")
	if !ok {
		log.Error("[websocket] handleBinaryMessage get uid error, session not contains uid key")
		return
	}
	uid := uids.(int64)

	// 校验后直接发布 WebSocket 本次读取的字节
	g.dispatch(uid, incoming.GetService(), msg)
}

// 错误时调用
func (g *Gate) handleError(_ *melody.Session, err error) {
	log.Errorf("[websocket] error occurred, err: %v", err)
}

func (g *Gate) handleClose(_ *melody.Session, code int, reason string) error {
	log.Infof("[websocket] connection closed, code: %v, reason: %v", code, reason)
	return nil
}

func sessionRemoteAddr(s *melody.Session) string {
	if s == nil {
		return ""
	}
	conn := s.WebsocketConnection()
	if conn == nil || conn.RemoteAddr() == nil {
		return ""
	}
	return conn.RemoteAddr().String()
}
