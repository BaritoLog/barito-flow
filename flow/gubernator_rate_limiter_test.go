package flow

import (
	"testing"
	"time"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestNewGubernatorRateLimiter_InterfaceCompliance(t *testing.T) {
	var _ RateLimiter = newGubernatorRateLimiter("127.0.0.1:10011")
}

func TestGubernatorRateLimiter_IsHitLimit_SingleNode(t *testing.T) {
	var maxToken int32 = 10
	peers := []string{
		"127.0.0.1:10011",
	}

	limiter := newGubernatorRateLimiter(peers[0])
	limiter.SetPeers(peers)
	limiter.Start()
	time.Sleep(10 * time.Millisecond)

	for i := maxToken; i > 0; i-- {
		FatalIf(t, limiter.IsHitLimit("abc", 1, maxToken), "it should be still have %d token(s) at abc", i)
	}
	FatalIf(t, !limiter.IsHitLimit("abc", 1, maxToken), "it should be hit limit at abc")
}
