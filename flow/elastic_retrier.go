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
	backoff     elastic.Backoff
	onRetryFunc func(err error)
}

func NewElasticRetrier(t time.Duration, f func(err error)) *ElasticRetrier {
	return &ElasticRetrier{
		backoff:     elastic.NewConstantBackoff(t),
		onRetryFunc: f,
	}
}

func (r *ElasticRetrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {

	log.Warn(errors.New(fmt.Sprintf("Elasticsearch Retrier #%d", retry)))

	if err == syscall.ECONNREFUSED {
		err = errors.New("Elasticsearch or network down")
	}

	// Let the backoff strategy decide how long to wait and whether to stop
	wait, stop := r.backoff.Next(retry)
	r.onRetryFunc(err)
	return wait, stop, nil
}
