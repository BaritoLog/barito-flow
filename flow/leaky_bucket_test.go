package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestLeakyBucket(t *testing.T) {
	max := 4

	bucket := NewLeakyBucket(max)
	FatalIf(t, bucket.Max() != max, "bucket.Max() is wrong")
	FatalIf(t, bucket.Token() != max, "bucket.Token() is wrong")

	for i := 0; i < max; i++ {
		FatalIf(t, !bucket.Take(), "bucket still have token")
	}

	FatalIf(t, bucket.Take(), "bucket is empty")

	bucket.Refill()
	FatalIf(t, !bucket.IsFull(), "bucket must be full")
	FatalIf(t, !bucket.Take(), "bucket is refilled")
}
