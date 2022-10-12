package flow_test

import (
	"fmt"
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/go-redis/redismock/v8"
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
	distributedRateLimiterNumOfWorkers    int    = 3
	// run 3 times more than `distributedRateLimiterDefaultMaxToken` to check is limitation reached
	distributedRateLimiterNumOfIteration int = int(distributedRateLimiterDefaultMaxToken) + 3
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// getKey returns formatted topic with iteration
func getKey(t *testing.T, iteration int) string {
	t.Helper()
	return fmt.Sprintf("%s:%d", distributedRateLimiterDefaultTopic, iteration)
}

// configureMock sets expectations
func configureMock(t *testing.T, mock redismock.ClientMock) {
	t.Helper()

	mock.MatchExpectationsInOrder(false)

	// redismock limitation: expectation should be set for different topics
	for i := 1; i <= distributedRateLimiterNumOfIteration; i++ {
		key := getKey(t, i)
		mock.ExpectGet(key).SetVal(fmt.Sprintf("%d", i))

		mock.ExpectTxPipeline()
		mock.ExpectIncrBy(key, 1).RedisNil()
		mock.ExpectExpire(key, time.Second).RedisNil()
		mock.ExpectTxPipelineExec().SetErr(nil)
	}

}

// work check isHitLimit
func work(t *testing.T, idx int, limiter flow.RateLimiter, iterationCh <-chan int, wg *sync.WaitGroup) {
	t.Helper()
	r := require.New(t)

	max := int(distributedRateLimiterDefaultMaxToken)

	for i := range iterationCh {
		key := getKey(t, i)
		isReachedLimit := limiter.IsHitLimit(
			key,
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

// TestDistributedRateLimiter_IsHitLimit tests rate limiter with multiple replicas
// simulated by goroutine
func TestDistributedRateLimiter_IsHitLimit(t *testing.T) {
	db, mock := redismock.NewClientMock()
	defer db.Close()

	configureMock(t, mock)

	limiter := flow.NewDistributedRateLimiter(db,
		flow.WithDuration(time.Second),
		flow.WithTimeout(time.Second),
		flow.WithMutex(),
	)

	iterationCh := make(chan int, distributedRateLimiterNumOfWorkers)
	defer close(iterationCh)

	wg := &sync.WaitGroup{}
	go func() {
		timeout := 10 * time.Second
		time.Sleep(timeout)
		close(iterationCh)

		log.Debugf("iteration stucks for %vs, something wrong with the worker / redis", timeout.Seconds())
	}()

	// simulate multiple replicas
	for i := 0; i < distributedRateLimiterNumOfWorkers; i++ {
		go work(t, i, limiter, iterationCh, wg)
	}

	for i := 1; i <= distributedRateLimiterNumOfIteration; i++ {
		iterationCh <- i
		wg.Add(1)
	}

	wg.Wait()
}