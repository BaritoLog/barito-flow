package flow

import (
	"testing"
	"time"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestNewGubernatorRateLimiter_InterfaceCompliance(t *testing.T) {
	var _ RateLimiter = newGubernatorRateLimiter("127.0.0.1:10010")
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

func TestGubernatorRateLimiter_IsHitLimit_MultipleNode(t *testing.T) {
	var maxToken int32 = 10
	peers := []string{
		"127.0.0.1:10012",
		"127.0.0.1:10013",
	}

	limiter1 := newGubernatorRateLimiter(peers[0])
	limiter2 := newGubernatorRateLimiter(peers[1])
	limiter1.SetPeers(peers)
	limiter2.SetPeers(peers)
	limiter1.Start()
	limiter2.Start()

	for i := maxToken; i > 0; i-- {
		time.Sleep(5 * time.Millisecond)
		FatalIf(t, limiter1.IsHitLimit("abc", 1, maxToken), "it should be still have %d token(s) at abc", i)
	}
	time.Sleep(10 * time.Millisecond)
	FatalIf(t, !limiter2.IsHitLimit("abc", 1, maxToken), "it should be hit limit at abc")
}

func TestGubernatorRateLimiter_IsHitLimit_MultipleTopic(t *testing.T) {
	var maxToken int32 = 10
	peers := []string{
		"127.0.0.1:10014",
	}

	limiter := newGubernatorRateLimiter(peers[0])
	limiter.SetPeers(peers)
	limiter.Start()
	time.Sleep(10 * time.Millisecond)

	for i := maxToken; i > 0; i-- {
		FatalIf(t, limiter.IsHitLimit("abc", 1, maxToken), "it should be still have %d token(s) at abc", i)
	}
	FatalIf(t, limiter.IsHitLimit("def", 1, maxToken), "it should be still have token(s) at def")
}

func TestGubernatorRateLimiter_IsHitLimit_MultipleHits(t *testing.T) {
	var maxToken int32 = 10
	peers := []string{
		"127.0.0.1:10015",
	}

	limiter := newGubernatorRateLimiter(peers[0])
	limiter.SetPeers(peers)
	limiter.Start()
	time.Sleep(10 * time.Millisecond)

	FatalIf(t, limiter.IsHitLimit("abc", int(maxToken), maxToken), "it should be still have %d tokens at abc", maxToken)
	FatalIf(t, !limiter.IsHitLimit("abc", 1, maxToken), "it should be hit limit at abc")
}

func TestGubernatorRateLimiter_IsHitLimit_ChangingMaxToken(t *testing.T) {
	var maxToken int32 = 10
	var newMaxToken int32 = 20
	peers := []string{
		"127.0.0.1:10016",
	}

	limiter := newGubernatorRateLimiter(peers[0])
	limiter.SetPeers(peers)
	limiter.Start()
	time.Sleep(10 * time.Millisecond)

	FatalIf(t, limiter.IsHitLimit("abc", int(maxToken), maxToken), "it should be still have %d tokens at abc", maxToken)
	time.Sleep(1 * time.Second)
	FatalIf(t, limiter.IsHitLimit("abc", int(newMaxToken), newMaxToken), "it should be still have %d tokens at abc", newMaxToken)
}

func TestGubernatorRateLimiter_IsStart(t *testing.T) {
	limiter := newGubernatorRateLimiter("127.0.0.1:10017")
	FatalIf(t, limiter.IsStart(), "it should be stopped")

	limiter.Start()
	FatalIf(t, !limiter.IsStart(), "it should be started")
}

func TestGubernatorRateLimiter_Stop_ReleaseResources(t *testing.T) {
	limiter := newGubernatorRateLimiter("127.0.0.1:10018")
	limiter.Start()
	limiter.Stop()
	time.Sleep(10 * time.Millisecond)

	limiter = newGubernatorRateLimiter("127.0.0.1:10018")
	time.Sleep(10 * time.Millisecond)
}
