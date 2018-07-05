package flow

import (
	"sync"
	"time"
)

type LeakyBucket interface {
	Close()
	StartRefill()
	Take() bool
	Token() int
	Max() int
}

type leakyBucket struct {
	max   int
	token int

	tick <-chan time.Time
	stop chan int
	lock sync.Mutex
}

func NewLeakyBucket(max int, duration time.Duration) LeakyBucket {

	return &leakyBucket{
		max:   max,
		token: max,
		tick:  time.Tick(duration),
		stop:  make(chan int),
	}
}

func (b *leakyBucket) Token() int {
	return b.token
}

func (b *leakyBucket) Max() int {
	return b.max
}

func (b *leakyBucket) StartRefill() {
	go b.loopRefillBucket()

}

func (l *leakyBucket) loopRefillBucket() {
	for {
		select {
		case <-l.tick:
			l.refillBucket()
		case <-l.stop:
			return
		}
	}
}

func (l *leakyBucket) refillBucket() {
	l.token = l.max
}

// TODO: take per topic
func (l *leakyBucket) Take() bool {

	if l.token <= 0 {
		return false
	}

	l.lock.Lock()
	l.token--
	l.lock.Unlock()
	return true
}

func (l *leakyBucket) Close() {
	go func() {
		l.stop <- 1
	}()
}
