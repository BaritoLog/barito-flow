package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestRateLimiter_IsHitMax_CreateNewBucketIfNotExist(t *testing.T) {
	limiter := &rateLimiter{
		duration:  1,
		bucketMap: make(map[string]*LeakyBucket),
	}

	isHit := limiter.IsHitLimit("some-topic", 1, 13)

	_, ok := limiter.bucketMap["some-topic"]
	FatalIf(t, !ok, "must be create new bucket with key some-topic")
	FatalIf(t, isHit, "new bucket must be full")
}

func TestRateLimiter(t *testing.T) {
	max := int32(5)

	limiter := NewRateLimiter(1)
	limiter.PutBucket("abc", NewLeakyBucket(max))
	limiter.PutBucket("def", NewLeakyBucket(max))
	limiter.Start()

	timekit.Sleep("1ms")
	FatalIf(t, !limiter.IsStart(), "limiter should be start")

	for i := int32(0); i < max; i++ {
		FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", i)
	}

	FatalIf(t, !limiter.IsHitLimit("abc", 1, max), "it should be hit limit at abc")
	FatalIf(t, limiter.IsHitLimit("def", 1, max), "it should be still have token at def")

	// wait until refill time
	timekit.Sleep("2s")
	FatalIf(t, !limiter.Bucket("abc").IsFull(), "bucket must be full")
	FatalIf(t, !limiter.Bucket("def").IsFull(), "bucket must be full")

	limiter.Stop()
	timekit.Sleep("1ms")
	FatalIf(t, limiter.IsStart(), "limiter should be stop")
}

func TestRateLimiter_Batch(t *testing.T) {
	max := int32(5)

	limiter := NewRateLimiter(1)
	limiter.PutBucket("abc", NewLeakyBucket(max))
	limiter.Start()

	timekit.Sleep("1ms")
	FatalIf(t, !limiter.IsStart(), "limiter should be start")

	FatalIf(t, !limiter.IsHitLimit("abc", 6, max), "it should be hit limit at abc")

	// wait until refill time
	timekit.Sleep("1s")
	FatalIf(t, !limiter.Bucket("abc").IsFull(), "bucket must be full")

	limiter.Stop()
	timekit.Sleep("1ms")
	FatalIf(t, limiter.IsStart(), "limiter should be stop")
}

func TestRateLimiter_IsHitLimit_UpdateMax(t *testing.T) {
	max := int32(4)
	newMax := int32(6)
	limiter := NewRateLimiter(1)
	limiter.PutBucket("abc", NewLeakyBucket(max))
	limiter.Start()

	timekit.Sleep("1s")
	FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", 0)
	FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", 1)
	FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", 2)
	FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", 3)

	FatalIf(t, limiter.IsHitLimit("abc", 1, newMax), "it should be still have token at abc: %d", 4)
}
