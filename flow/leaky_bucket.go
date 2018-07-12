package flow

import (
	"sync"
)

type LeakyBucket struct {
	max   int
	token int
	lock  sync.Mutex
}

func NewLeakyBucket(max int) *LeakyBucket {
	return &LeakyBucket{
		max:   max,
		token: max,
	}
}

func (b *LeakyBucket) Token() int {
	return b.token
}

func (b *LeakyBucket) Max() int {
	return b.max
}

func (b *LeakyBucket) IsFull() bool {
	return b.token == b.max
}

func (l *LeakyBucket) Refill() {
	l.token = l.max
}

func (l *LeakyBucket) Take() bool {

	if l.token <= 0 {
		return false
	}

	l.lock.Lock()
	l.token--
	l.lock.Unlock()
	return true
}
