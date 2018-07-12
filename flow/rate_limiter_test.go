package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestRateLimiter_IsHitMax_CreateNewBucketIfNotExist(t *testing.T) {
	limiter := &rateLimiter{
		bucketMap: make(map[string]*LeakyBucket),
	}

	isHit := limiter.IsHitLimit("some-topic", 13)

	_, ok := limiter.bucketMap["some-topic"]
	FatalIf(t, !ok, "must be create new bucket with key some-topic")
	FatalIf(t, isHit, "new bucket must be full")
}

func TestRateLimiter(t *testing.T) {
	max := 5

	limiter := NewRateLimiter(timekit.Duration("1ms"))
	limiter.PutBucket("abc", NewLeakyBucket(max))
	limiter.PutBucket("def", NewLeakyBucket(max))
	limiter.Start()

	timekit.Sleep("1ns")
	FatalIf(t, !limiter.IsStart(), "limiter should be start")

	for i := 0; i < max; i++ {
		FatalIf(t, limiter.IsHitLimit("abc", max), "it should be still have token at abc: %s", i)
	}

	FatalIf(t, !limiter.IsHitLimit("abc", max), "it should be hit limit at abc")
	FatalIf(t, limiter.IsHitLimit("def", max), "it should be still have token at def")

	// wait until refill time
	timekit.Sleep("2ms")
	FatalIf(t, !limiter.Bucket("abc").IsFull(), "bucket must be full")
	FatalIf(t, !limiter.Bucket("def").IsFull(), "bucket must be full")

	limiter.Stop()
	timekit.Sleep("1ms")
	FatalIf(t, limiter.IsStart(), "limiter should be stop")
}
