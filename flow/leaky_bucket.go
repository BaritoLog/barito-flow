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

func (b *LeakyBucket) UpdateMax(newMax int) {
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

	if (l.token - count) < 0 {
		return false
	}

	l.lock.Lock()
	l.token = l.token - count
	l.lock.Unlock()
	return true
}
