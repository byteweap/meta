package gate

import (
	"sync"

	"github.com/olahol/melody"
)

// Sessions 管理所有会话
type Sessions struct {
	data  sync.Map
	mu    sync.Mutex
	locks map[int64]*sessionLock
}

type sessionLock struct {
	mu    sync.Mutex
	users int
}

func newSessions() *Sessions {
	return &Sessions{locks: make(map[int64]*sessionLock)}
}

// lock 将同一用户的远端归属与本地连接切换串行化；空闲锁立即回收。
// 不能在持有用户锁时递归调用 lock。
func (ss *Sessions) lock(uid int64) func() {
	ss.mu.Lock()
	l := ss.locks[uid]
	if l == nil {
		l = &sessionLock{}
		ss.locks[uid] = l
	}
	l.users++
	ss.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		ss.mu.Lock()
		l.users--
		if l.users == 0 {
			delete(ss.locks, uid)
		}
		ss.mu.Unlock()
	}
}

// replace 注册新会话并返回旧会话
func (ss *Sessions) replace(uid int64, s *melody.Session) (*melody.Session, bool) {
	old, loaded := ss.data.Swap(uid, s)
	if !loaded {
		return nil, false
	}
	return old.(*melody.Session), true
}

// unregisterIfSame 仅当当前会话匹配时注销
func (ss *Sessions) unregisterIfSame(uid int64, s *melody.Session) bool {
	return ss.data.CompareAndDelete(uid, s)
}

// get 获取会话
func (ss *Sessions) get(uid int64) (*melody.Session, bool) {
	if session, ok := ss.data.Load(uid); ok {
		return session.(*melody.Session), true
	}
	return nil, false
}
