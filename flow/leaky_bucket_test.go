package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestLeakyBucket(t *testing.T) {
	max := 4
	duration := timekit.Duration("3ms")

	bucket := NewLeakyBucket(max, duration)
	defer bucket.Close()

	go bucket.StartRefill()

	for i := 0; i < max; i++ {
		FatalIf(t, !bucket.Take(), "bucket still have token")
	}

	FatalIf(t, bucket.Take(), "bucket is empty")

	timekit.Sleep("4ms")
	FatalIf(t, bucket.Token() != bucket.Max(), "bucket must be refill")
	FatalIf(t, !bucket.Take(), "bucket is refilled")
}

func TestLeakyBucket_IsThreadSafe(t *testing.T) {
	max := 20
	bucket := NewLeakyBucket(max, timekit.Duration("1s"))

	for i := 0; i < max; i++ {
		go bucket.Take()
	}

	timekit.Sleep("1ms")
	FatalIf(t, bucket.Token() > 0, "bucket must be empty")
}
