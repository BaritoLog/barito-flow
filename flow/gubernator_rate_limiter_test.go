package flow

import "testing"

func TestNewGubernatorRateLimiter_InterfaceCompliance(t *testing.T) {
	var _ RateLimiter = newGubernatorRateLimiter("127.0.0.1:10011")
}
