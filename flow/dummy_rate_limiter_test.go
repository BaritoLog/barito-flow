package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestDummyRateLimiter(t *testing.T) {
	var v interface{} = NewDummyRateLimiter()
	limiter, ok := v.(RateLimiter)
	FatalIf(t, !ok, "listener must be implemnet of RateLimiter")

	bucket := limiter.Bucket("topic")
	FatalIf(t, bucket != nil, "wrong limiter.Bucket()")

	isHit := limiter.IsHitLimit("topic", 1, 1)
	FatalIf(t, isHit, "wrong limiter.IsHitLimit()")

	isStart := limiter.IsStart()
	FatalIf(t, isStart, "wrong limiter.IsStart()")

	limiter.PutBucket("topic", nil)
	limiter.Start()
	limiter.Stop()

}
