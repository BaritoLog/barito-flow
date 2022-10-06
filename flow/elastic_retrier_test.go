package flow

import (
	"context"
	"testing"
	"time"
)

const (
	MAX_RETRY = 10
)

func mockElasticRetrier() *ElasticRetrier {
	return NewElasticRetrier(1*time.Second, MAX_RETRY, onRetry, func() {})
}

func onRetry(err error) {
	// Nothing to do
}

func TestNewElasticRetrier(t *testing.T) {
	r := NewElasticRetrier(1*time.Second, MAX_RETRY, onRetry, func() {})
	wait, ok, err := r.Retry(context.TODO(), 1, nil, nil, nil)
	if want, got := 1*time.Second, wait; want != got {
		t.Fatalf("expected %v, got %v", want, got)
	}

	want := true //Loop
	if got := ok; want != got {
		t.Fatalf("expected %v, got %v", want, got)
	}

	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestNewElasticRetrierReachedMax(t *testing.T) {
	reachedMax := false
	onRetryCounter := 0

	r := NewElasticRetrier(1*time.Millisecond, MAX_RETRY, func(err error) { onRetryCounter++ }, func() { reachedMax = true })
	for i := 1; i < 10; i++ {
		r.Retry(context.TODO(), i, nil, nil, nil)
		if reachedMax {
			t.Fatalf("reachedMax should be false on %d iteration, got %v", i, reachedMax)
		}
	}
	r.Retry(context.TODO(), 10, nil, nil, nil)

	if !reachedMax {
		t.Fatalf("reachedMax should be true, got %v", reachedMax)
	}
	if onRetryCounter != 10 {
		t.Fatalf("onRetryCounter should be 10, got %v", onRetryCounter)
	}
}
