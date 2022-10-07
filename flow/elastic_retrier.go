package flow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"syscall"
	"time"

	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
)

type ElasticRetrier struct {
	backoff           elastic.Backoff
	onRetryFunc       func(err error)
	onMaxRetryReached func()
	maxRetry          int
}

func NewElasticRetrier(t time.Duration, n int, onRetryFunc func(err error), onMaxRetryReached func()) *ElasticRetrier {
	return &ElasticRetrier{
		backoff:           elastic.NewConstantBackoff(t),
		onRetryFunc:       onRetryFunc,
		onMaxRetryReached: onMaxRetryReached,
		maxRetry:          n,
	}
}

func (r *ElasticRetrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {
	log.Warn(errors.New(fmt.Sprintf("Elasticsearch Retrier #%d", retry)))

	if err == syscall.ECONNREFUSED {
		err = errors.New("Elasticsearch or network down")
	}

	// Let the backoff strategy decide how long to wait and whether to shouldRetry
	wait, shouldRetry := r.backoff.Next(retry)
	r.onRetryFunc(err)

	// if max retry 0, it will retry forever
	if r.maxRetry > 0 && retry >= r.maxRetry {
		shouldRetry = false
		r.onMaxRetryReached()
	}

	return wait, shouldRetry, nil
}
