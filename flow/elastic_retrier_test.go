package flow

import (
	"context"
	"testing"
	"time"
)

func mockElasticRetrier() *ElasticRetrier {
	maxRetry := 10
	return NewElasticRetrier(1*time.Second, maxRetry, mockRetrier)
}

func mockRetrier(err error) {
	// Nothing to do
}

func TestNewElasticRetrier(t *testing.T) {
	maxRetry := 10
	r := NewElasticRetrier(1*time.Second, maxRetry, mockRetrier)
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
