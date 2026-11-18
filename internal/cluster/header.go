package cluster

import (
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/pkg/conv"
)

const (
	FieldNameUID         = "uid"
	FieldNameEvent       = "event"
	FieldNameReply       = "reply"
	FieldNameFromService = "from_service"
	FieldNameToService   = "to_service"
	FieldNameSessionID   = "session_id"
)

const (
	// FieldNameDelivery 是 Gate 下行的投递方式头字段
	FieldNameDelivery = "delivery"
	// DeliveryResponse 表示绑定原连接的响应
	DeliveryResponse = "response"
	// DeliveryPush 表示只按 UID 投递给当前连接的推送
	DeliveryPush = "push"
)

// BuildHeader 构建必备请求头
func BuildHeader(uid int64, event Event, reply, fromService, toService string) broker.Header {
	return broker.Header{
		FieldNameUID:         []string{conv.String(uid)},
		FieldNameEvent:       []string{string(event)},
		FieldNameReply:       []string{reply},
		FieldNameFromService: []string{fromService},
		FieldNameToService:   []string{toService},
	}
}

// GetUIDBy 从请求头中获取用户ID
func GetUIDBy(header broker.Header) int64 {
	return conv.Int64(header.Get(FieldNameUID))
}

// GetEventBy 从请求头中获取事件类型
func GetEventBy(header broker.Header) Event {
	return Event(header.Get(FieldNameEvent))
}

// GetReplyBy 从请求头中获取回复信息
func GetReplyBy(header broker.Header) string {
	return header.Get(FieldNameReply)
}

// GetFromServiceBy 从请求头中获取来源服务
func GetFromServiceBy(header broker.Header) string {
	return header.Get(FieldNameFromService)
}

// GetToServiceBy 从请求头中获取目标服务
func GetToServiceBy(header broker.Header) string {
	return header.Get(FieldNameToService)
}

// GetSessionIDBy 返回网关会话标识。头部未提供时返回空字符串
func GetSessionIDBy(header broker.Header) string {
	return header.Get(FieldNameSessionID)
}
