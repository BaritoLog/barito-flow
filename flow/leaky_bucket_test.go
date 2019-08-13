package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestLeakyBucket(t *testing.T) {
	max := int32(4)

	bucket := NewLeakyBucket(max)
	FatalIf(t, bucket.Max() != max, "bucket.Max() is wrong")
	FatalIf(t, bucket.Token() != max, "bucket.Token() is wrong")

	for i := int32(0); i < max; i++ {
		FatalIf(t, !bucket.Take(1), "bucket still have token")
	}

	FatalIf(t, bucket.Take(1), "bucket is empty")

	bucket.Refill()
	FatalIf(t, !bucket.IsFull(), "bucket must be full")
	FatalIf(t, !bucket.Take(1), "bucket is refilled")
}

func TestUpdateMax(t *testing.T) {
	max := int32(4)
	bucket := NewLeakyBucket(max)

	bucket.Take(2)
	bucket.UpdateMax(6)
	bucket.Take(2)
	FatalIf(t, !bucket.Take(1), "bucket is empty")
}
