package flow

type dummyLeakyBucket struct {
	take  bool
	token int
	max   int
}

func (b *dummyLeakyBucket) Close()       {}
func (b *dummyLeakyBucket) StartRefill() {}
func (b *dummyLeakyBucket) Take() bool   { return b.take }
func (b *dummyLeakyBucket) Token() int   { return b.token }
func (b *dummyLeakyBucket) Max() int     { return b.max }
