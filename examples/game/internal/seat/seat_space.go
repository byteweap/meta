package seat

import "errors"

// Space 是固定容量的座位空间，由调用方串行访问。
type Space struct {
	seats []*Seat
}

// NewSpace 创建座位空间，容量可以为零，但不能为负数。
func NewSpace(size int) (*Space, error) {
	if size < 0 {
		return nil, errors.New("seat space size must not be negative")
	}
	return &Space{
		seats: make([]*Seat, size),
	}, nil
}

// GetSeat 获取座位
func (s *Space) GetSeat(id int) (*Seat, bool) {
	if id < 0 || id >= len(s.seats) {
		return nil, false
	}
	seat := s.seats[id]
	return seat, seat != nil
}

// Register 注册座位
func (s *Space) Register(seat *Seat) error {
	if seat == nil {
		return errors.New("seat must not be nil")
	}
	id := seat.id
	if id < 0 || id >= len(s.seats) {
		return errors.New("seat ID out of range")
	}
	if s.seats[id] != nil {
		return errors.New("seat already exists")
	}
	s.seats[id] = seat
	return nil
}

// Unregister 注销座位
func (s *Space) Unregister(id int) {
	if id < 0 || id >= len(s.seats) {
		return
	}
	s.seats[id] = nil
}
