package flow_test

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

const (
	distributedRateLimiterDefaultTopic    string = "foo"
	distributedRateLimiterDefaultCount    int    = 1
	distributedRateLimiterDefaultMaxToken int32  = 10
	distributedRateLimiterNumOfWorkers    int    = 5
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// work check isHitLimit
func work(t *testing.T, idx int, limiter flow.RateLimiter, iterationCh <-chan int, wg *sync.WaitGroup) {
	t.Helper()
	r := require.New(t)

	max := int(distributedRateLimiterDefaultMaxToken)

	for i := range iterationCh {
		isReachedLimit := limiter.IsHitLimit(
			distributedRateLimiterDefaultTopic,
			distributedRateLimiterDefaultCount,
			distributedRateLimiterDefaultMaxToken,
		)

		log.Debugf("worker-%v got signal for iteration: %v, is reached limit? %v", idx, i, isReachedLimit)

		if i > max && isReachedLimit != true {
			r.Equalf(true, isReachedLimit, "at iteration: %v", i)
		}

		if i < max && isReachedLimit != false {
			r.Equalf(false, isReachedLimit, "at iteration: %v", i)
		}

		wg.Done()
	}
}

func TestDistributedRateLimiter_IsHitLimit(t *testing.T) {
	db := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// @TODO using redismock
	//db, _ := redismock.NewClientMock()
	defer db.Close()

	limiter := flow.NewDistributedRateLimiter(db,
		flow.WithDuration(time.Second),
		flow.WithTimeout(time.Second),
		flow.WithMutex(),
	)

	iterationCh := make(chan int, distributedRateLimiterNumOfWorkers)
	defer close(iterationCh)

	wg := &sync.WaitGroup{}

	// simulate multiple replicas
	for i := 0; i < distributedRateLimiterNumOfWorkers; i++ {
		go work(t, i, limiter, iterationCh, wg)
	}

	// run 3 times more than `distributedRateLimiterDefaultMaxToken` to check is limitation reached
	for i := 1; i <= int(distributedRateLimiterDefaultMaxToken)+3; i++ {
		iterationCh <- i
		wg.Add(1)
	}

	wg.Wait()
}
