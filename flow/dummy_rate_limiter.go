package flow

type dummyRateLimiter struct {
	IsHitLimitFunc func(topic string, count int, maxTokenIfNotExist int32) bool
	StartFunc      func()
	StopFunc       func()
	IsStartFunc    func() bool
	PutBucketFunc  func(topic string, bucket *LeakyBucket)
	BucketFunc     func(topic string) *LeakyBucket
}

func NewDummyRateLimiter() *dummyRateLimiter {
	return &dummyRateLimiter{
		IsHitLimitFunc: func(topic string, count int, maxTokenIfNotExist int32) bool { return false },
		StartFunc:      func() {},
		StopFunc:       func() {},
		IsStartFunc:    func() bool { return false },
		PutBucketFunc:  func(topic string, bucket *LeakyBucket) {},
		BucketFunc:     func(topic string) *LeakyBucket { return nil },
	}

}

func (l *dummyRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	return l.IsHitLimitFunc(topic, count, maxTokenIfNotExist)
}
func (l *dummyRateLimiter) Start() {
	l.StartFunc()
}
func (l *dummyRateLimiter) Stop() {
	l.StopFunc()
}
func (l *dummyRateLimiter) IsStart() bool {
	return l.IsStartFunc()
}
func (l *dummyRateLimiter) PutBucket(topic string, bucket *LeakyBucket) {
	l.PutBucketFunc(topic, bucket)
}
func (l *dummyRateLimiter) Bucket(topic string) *LeakyBucket {
	return l.BucketFunc(topic)
}

func (l *dummyRateLimiter) Expect_IsHitLimit_AlwaysTrue() {
	l.IsHitLimitFunc = func(topic string, count int, maxTokenIfNotExist int32) bool {
		return true
	}
}
