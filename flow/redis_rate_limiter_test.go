package flow_test

import (
	"fmt"
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	distributedRateLimiterDefaultTopic    string        = "foo"
	distributedRateLimiterDefaultCount    int           = 2
	distributedRateLimiterDefaultMaxToken int32         = 3
	distributedRateLimiterNumOfWorkers    int           = 2
	distributedRateLimiterDuration        time.Duration = time.Second * 2
)

var (
	// run 3 times more than the cap to check is limitation reached
	distributedRateLimiterNumOfIteration int = 3 + int(distributedRateLimiterDefaultMaxToken)*int(distributedRateLimiterDuration.Seconds())
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

		mock.ExpectIncrBy(key, int64(distributedRateLimiterDefaultCount)).
			SetVal(int64(i + distributedRateLimiterDefaultCount))
		mock.ExpectExpire(key, distributedRateLimiterDuration).RedisNil()
	}

}

// work check isHitLimit
func work(t *testing.T, idx int, limiter flow.RateLimiter, iterationCh <-chan int, wg *sync.WaitGroup) {
	t.Helper()
	r := require.New(t)

	max := int(distributedRateLimiterDefaultMaxToken) * int(distributedRateLimiterDuration.Seconds())
	log.Debug("max:", max)

	for i := range iterationCh {
		key := getKey(t, i)
		isReachedLimit := limiter.IsHitLimit(
			key,
			distributedRateLimiterDefaultCount,
			distributedRateLimiterDefaultMaxToken,
		)

		log.Debugf("worker-%v got signal for iteration: %v, is reached limit? %v", idx, i, isReachedLimit)

		if i+distributedRateLimiterDefaultCount > max && isReachedLimit != true {
			r.Equalf(true, isReachedLimit, "at iteration: %v", i)
		}

		if i+distributedRateLimiterDefaultCount < max && isReachedLimit != false {
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

	limiter := flow.NewRedisRateLimiter(db,
		flow.WithDuration(distributedRateLimiterDuration),
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
		os.Exit(1)
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

type mockRateLimiter struct {
	IsStartAttr       bool
	IsStop            bool
	IsHitLimitCounter int
}

func (m *mockRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	m.IsHitLimitCounter++
	return false
}

func (m *mockRateLimiter) Start() {
	m.IsStartAttr = true
}

func (m *mockRateLimiter) Stop() {
	m.IsStartAttr = false
	m.IsStop = true
}

func (m *mockRateLimiter) IsStart() bool {
	return m.IsStartAttr
}

func (m *mockRateLimiter) PutBucket(_ string, _ *flow.LeakyBucket) {
}

func (m *mockRateLimiter) Bucket(_ string) *flow.LeakyBucket {
	return nil
}

func TestDistributedRateLimiter_LocalRateLimiter(t *testing.T) {
	r := require.New(t)

	// simulate connection refused
	db := redis.NewClient(&redis.Options{Addr: "[::1]:14045"})
	mock := &mockRateLimiter{}
	var localLimiter flow.RateLimiter = mock

	limiter := flow.NewRedisRateLimiter(db,
		flow.WithFallbackToLocal(localLimiter),
	)

	r.Equal(false, limiter.IsStart())
	r.Equal(false, mock.IsStop)

	limiter.Start()
	r.Equal(true, limiter.IsStart())

	limiter.Stop()
	r.Equal(false, limiter.IsStart())
	r.Equal(true, mock.IsStop)
}

func TestDistributedRateLimiter_LocalRateLimiter_IsHitLimit(t *testing.T) {
	r := require.New(t)

	// simulate connection refused
	db := redis.NewClient(&redis.Options{Addr: "[::1]:14045"})
	mock := &mockRateLimiter{}
	var localLimiter flow.Limiter = mock

	limiter := flow.NewRedisRateLimiter(db,
		flow.WithFallbackToLocal(localLimiter),
	)

	r.Equal(0, mock.IsHitLimitCounter)

	isHitLimit := limiter.IsHitLimit("random", 1, 1)

	// first hit will got limit
	r.Equal(true, isHitLimit)
	r.Equal(0, mock.IsHitLimitCounter)

	isHitLimit = limiter.IsHitLimit("random", 1, 1)

	// second hit will use local rate limiter
	r.Equal(false, isHitLimit)
	r.Equal(1, mock.IsHitLimitCounter)
}
