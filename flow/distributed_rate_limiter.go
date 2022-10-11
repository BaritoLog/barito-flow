package flow

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	// defaultTimeout is the default context timeout for querying to redis
	defaultTimeout time.Duration = time.Second

	// defaultDuration is the default key duration used by EXPIRE command
	defaultDuration time.Duration = time.Minute
)

// DistributedRateLimiterOpts is used to set optional attributes of DistributedRateLimiter
type DistributedRateLimiterOpts func(d *DistributedRateLimiter)

// WithDuration sets topic expiration
// otherwise defaultDuration will be used
func WithDuration(duration time.Duration) DistributedRateLimiterOpts {
	return func(d *DistributedRateLimiter) {
		d.duration = duration
	}
}

// WithTimeout sets context timeout duration
// otherwise defaultTimeout will be used
func WithTimeout(timeout time.Duration) DistributedRateLimiterOpts {
	return func(d *DistributedRateLimiter) {
		d.timeout = timeout
	}
}

// WithMutex is used whenever DistributedRateLimiter is used inside a goroutine to avoid race condition
func WithMutex() DistributedRateLimiterOpts {
	return func(d *DistributedRateLimiter) {
		d.Mutex = &sync.Mutex{}
	}
}

// DistributedRateLimiter is a RateLimiter implementation
// which depends on Redis as a remote storage
type DistributedRateLimiter struct {
	*sync.Mutex
	db       *redis.Client
	timeout  time.Duration
	duration time.Duration
}

// NewDistributedRateLimiter creates *DistributedRateLimiter
func NewDistributedRateLimiter(db *redis.Client, opts ...DistributedRateLimiterOpts) *DistributedRateLimiter {
	d := &DistributedRateLimiter{
		db:       db,
		timeout:  defaultTimeout,
		duration: defaultDuration,
	}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

// IsHitLimit implements best practice from redis official website
// https://redis.com/redis-best-practices/basic-rate-limiting/
func (d *DistributedRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	if d.Mutex != nil {
		d.Lock()
		defer d.Unlock()
	}

	currentToken, err := d.getTopicCounter(context.Background(), topic)
	if err != nil {
		log.Errorf("unexpected error: %v", err)
		return true
	}

	if currentToken >= maxTokenIfNotExist {
		log.Debugf("topic: %v is reaching limit", topic)
		return true
	}

	log.Debugf("current token: %v", currentToken)
	if err := d.incrementTopicCounterBy(context.Background(), topic, int64(count)); d.isLegitRedisError(err) {
		log.Errorf("unexpected error: %v", err)
		return true
	}

	return false
}

func (d *DistributedRateLimiter) Start() {
	// no-op
}

func (d *DistributedRateLimiter) Stop() {
	// no-op
}

func (d *DistributedRateLimiter) IsStart() bool {
	return true
}

func (d *DistributedRateLimiter) PutBucket(_ string, _ *LeakyBucket) {
	// no-op
}

func (d *DistributedRateLimiter) Bucket(_ string) *LeakyBucket {
	// no-op
	return nil
}

// getTopicCounter returns the current counter of the topic
// getTopicCounter will query redis using GET
// if GET query returns nil / topic is unset, getTopicCounter will return 0
// otherwise return the result / error
func (d *DistributedRateLimiter) getTopicCounter(ctx context.Context, topic string) (counter int32, err error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	result, err := d.db.Get(ctx, topic).Result()

	if err != nil {
		// if key is unset
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}

		// if legit error
		return 0, err
	}

	if _, err := fmt.Sscanf(result, "%d", &counter); err != nil {
		return 0, err
	}

	return counter, nil
}

// incrementTopicCounterBy creates a transaction which
// increments the topic by given value using INCRBY query
// if topic is newly created, TTL will be set using EXPIRE query with DistributedRateLimiter.duration
func (d *DistributedRateLimiter) incrementTopicCounterBy(ctx context.Context, topic string, value int64) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	_, err := d.db.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if err := p.IncrBy(ctx, topic, value).Err(); d.isLegitRedisError(err) {
			return err
		}

		if err := p.ExpireNX(ctx, topic, d.duration).Err(); d.isLegitRedisError(err) {
			return err
		}

		return nil
	})

	return err
}

// isLegitRedisError checks whether given err is truly redis error, and not Nil returned by redis
func (d *DistributedRateLimiter) isLegitRedisError(err error) bool {
	return err != nil && !errors.Is(err, redis.Nil)
}
