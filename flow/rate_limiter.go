package flow

import (
	"time"
)

type RateLimiter interface {
	IsHitLimit(topic string, maxTokenIfNotExist int) bool
	Start()
	Stop()
	IsStart() bool
	PutBucket(topic string, bucket *LeakyBucket)
	Bucket(topic string) *LeakyBucket
}

type rateLimiter struct {
	isStart   bool
	tick      <-chan time.Time
	stop      chan int
	bucketMap map[string]*LeakyBucket
}

func NewRateLimiter(duration time.Duration) RateLimiter {
	return &rateLimiter{
		tick:      time.Tick(duration),
		stop:      make(chan int),
		bucketMap: make(map[string]*LeakyBucket),
	}
}

func (l *rateLimiter) IsHitLimit(topic string, maxTokenIfNotExist int) bool {
	bucket, ok := l.bucketMap[topic]
	if !ok {
		bucket = NewLeakyBucket(maxTokenIfNotExist)
		l.bucketMap[topic] = bucket
	}
	return !bucket.Take(1)
}

func (l *rateLimiter) Start() {
	go l.loopRefillBuckets()
}

func (l *rateLimiter) Stop() {
	go func() {
		l.stop <- 1
	}()
}

func (l *rateLimiter) IsStart() bool {
	return l.isStart
}

func (l *rateLimiter) PutBucket(topic string, bucket *LeakyBucket) {
	l.bucketMap[topic] = bucket
}

func (l *rateLimiter) Bucket(topic string) *LeakyBucket {
	return l.bucketMap[topic]
}

func (l *rateLimiter) loopRefillBuckets() {
	l.isStart = true
	for {
		select {
		case <-l.tick:
			l.refillBuckets()
		case <-l.stop:
			l.isStart = false
			return
		}
	}
}

func (l *rateLimiter) refillBuckets() {
	for _, bucket := range l.bucketMap {
		bucket.Refill()
	}
}
