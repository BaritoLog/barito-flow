package flow

type gubernatorRateLimiter struct {
}

func newGubernatorRateLimiter(grpcAddress string) *gubernatorRateLimiter {
	return &gubernatorRateLimiter{}
}

func (*gubernatorRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	return false
}

func (*gubernatorRateLimiter) IsStart() bool {
	return false
}

func (*gubernatorRateLimiter) Start() {}

func (*gubernatorRateLimiter) Stop() {}
