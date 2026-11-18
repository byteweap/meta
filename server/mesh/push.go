package mesh

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

// ErrUserOffline 表示定位器中没有该 UID 的在线 Gate 会话
var ErrUserOffline = errors.New("mesh: user offline")

// Push 按 UID 向当前 Gate 发布一次主动推送，返回 nil 表示 Broker 适配器已接受发布
// Mesh 须已就绪，uid 须为正数。payload 为 nil 时发送空消息体，用户离线时返回 ErrUserOffline
// ctx 必须非 nil。调用方取消或 Mesh 停止时向定位器和 Broker 传递取消信号
// 开始发布前会再检查截止时间。发布中的调用返回 Broker 的结果，用户在定位后换 Gate 时推送可能丢失
func (m *Mesh) Push(ctx context.Context, uid int64, cmd, version uint32, payload proto.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if uid <= 0 {
		return fmt.Errorf("mesh: invalid push UID %d", uid)
	}
	select {
	case <-m.ready:
	default:
		return errors.New("mesh: push requires a ready service")
	}
	if !validSubjectToken(m.appName) {
		return fmt.Errorf("mesh: invalid push source service %q", m.appName)
	}

	ctx, cleanup := outboundContext(ctx, m.ctx)
	defer cleanup.release()
	if err := m.pushContextErr(ctx); err != nil {
		return err
	}
	gate := m.opts.gateService
	session, err := m.opts.locator.Session(ctx, uid, gate)
	if err != nil {
		return err
	}
	if canceled := m.pushContextErr(ctx); canceled != nil {
		return canceled
	}
	if session.Node == "" {
		return ErrUserOffline
	}
	if !validSubjectToken(session.Node) {
		return fmt.Errorf("mesh: invalid Gate node %q", session.Node)
	}

	body, err := proto.Marshal(payload)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(&envelope.ServerEnvelope{
		Header:  &envelope.Header{Cmd: cmd, Version: version, SentAtUnixMs: time.Now().UnixMilli()},
		Service: m.appName,
		Kind:    envelope.MsgType_PUSH,
		Payload: body,
	})
	if err != nil {
		return err
	}
	header := cluster.BuildHeader(uid, cluster.EventBusiness, "", m.appName, gate)
	header.Set(cluster.FieldNameDelivery, cluster.DeliveryPush)
	subject := cluster.Subject(m.opts.prefix, m.appName, gate, session.Node)
	if canceled := m.pushContextErr(ctx); canceled != nil {
		return canceled
	}
	return m.opts.broker.Pub(ctx, subject, data, broker.PubHeader(header))
}

// pushContextErr 同步检查 Mesh 和有效期限，停服回调或截止定时器可能尚未得到调度
func (m *Mesh) pushContextErr(ctx context.Context) error {
	if err := m.ctx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= 0 {
		return context.DeadlineExceeded
	}
	return nil
}
