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
	return NewElasticRetrier(1*time.Second, MAX_RETRY, mockRetrier)
}

func mockRetrier(err error) {
	// Nothing to do
}

func TestNewElasticRetrier(t *testing.T) {
	r := NewElasticRetrier(1*time.Second, MAX_RETRY, mockRetrier)
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
