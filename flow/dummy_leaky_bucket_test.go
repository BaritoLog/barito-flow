package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestDummyLeakyBucket(t *testing.T) {
	var i interface{} = &dummyLeakyBucket{
		take:  true,
		token: 12,
		max:   13,
	}

	bucket, ok := i.(LeakyBucket)
	FatalIf(t, !ok, "dummyLeakyBucket must be implement of LeakyBucket")

	bucket.StartRefill()
	FatalIf(t, bucket.Take() != true, "take should be true")
	FatalIf(t, bucket.Token() != 12, "token should be true")
	FatalIf(t, bucket.Max() != 13, "max should be true")
	bucket.Close()
}
