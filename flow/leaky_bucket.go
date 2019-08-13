package flow

import (
	"sync"
)

type LeakyBucket struct {
	max   int32
	token int32
	lock  sync.Mutex
}

func NewLeakyBucket(max int32) *LeakyBucket {
	return &LeakyBucket{
		max:   max,
		token: max,
	}
}

func (b *LeakyBucket) Token() int32 {
	return b.token
}

func (b *LeakyBucket) Max() int32 {
	return b.max
}

func (b *LeakyBucket) UpdateMax(newMax int32) {
	b.lock.Lock()
	if newMax > b.max {
		b.token = b.token + (newMax - b.max)
	}
	b.max = newMax
	b.lock.Unlock()
}

func (b *LeakyBucket) IsFull() bool {
	return b.token == b.max
}

func (l *LeakyBucket) Refill() {
	l.token = l.max
}

func (l *LeakyBucket) Take(count int) bool {

	if (l.token - int32(count)) < 0 {
		return false
	}

	l.lock.Lock()
	l.token = l.token - int32(count)
	l.lock.Unlock()
	return true
}
