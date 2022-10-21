package flow

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

const (
	// defaultTimeout is the default context timeout for querying to redis
	defaultTimeout time.Duration = time.Second

	// defaultDuration is the default key duration used by EXPIRE command
	defaultDuration time.Duration = time.Minute

	// defaultKeyPrefix is the default key prefix
	defaultKeyPrefix string = ""
)

// DistributedRateLimiterOpts is used to set optional attributes of RedisRateLimiter
type DistributedRateLimiterOpts func(d *RedisRateLimiter)

// WithDuration sets topic expiration
// otherwise defaultDuration will be used
func WithDuration(duration time.Duration) DistributedRateLimiterOpts {
	return func(d *RedisRateLimiter) {
		d.duration = duration
	}
}

// WithTimeout sets context timeout duration
// otherwise defaultTimeout will be used
func WithTimeout(timeout time.Duration) DistributedRateLimiterOpts {
	return func(d *RedisRateLimiter) {
		d.timeout = timeout
	}
}

// WithKeyPrefix enables separation of key among redis usage
func WithKeyPrefix(prefix string) DistributedRateLimiterOpts {
	return func(d *RedisRateLimiter) {
		d.keyPrefix = strings.TrimSpace(prefix)
	}
}

// WithMutex is used whenever RedisRateLimiter is used inside a goroutine to avoid race condition
func WithMutex() DistributedRateLimiterOpts {
	return func(d *RedisRateLimiter) {
		d.Mutex = &sync.Mutex{}
	}
}

// RedisRateLimiter is a RateLimiter implementation
// which depends on Redis as a remote storage
type RedisRateLimiter struct {
	*sync.Mutex
	db        *redis.Client
	timeout   time.Duration
	duration  time.Duration
	keyPrefix string
}

// NewRedisRateLimiter creates *RedisRateLimiter
func NewRedisRateLimiter(db *redis.Client, opts ...DistributedRateLimiterOpts) *RedisRateLimiter {
	d := &RedisRateLimiter{
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
func (d *RedisRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	if d.Mutex != nil {
		d.Lock()
		defer d.Unlock()
	}

	key := d.getKeyFormatted(topic)
	currentToken, err := d.getKeyCurrentToken(context.Background(), key)
	if err != nil {
		log.Errorf("unexpected error: %v", err)
		return true
	}

	if (currentToken + int32(count)) > (maxTokenIfNotExist * int32(d.duration.Seconds())) {
		log.Debugf("key: %v is reaching limit", key)
		return true
	}

	log.Debugf("current token: %v", currentToken)
	if err := d.incrementKeyCounterBy(context.Background(), key, int64(count)); d.isLegitRedisError(err) {
		log.Errorf("unexpected error: %v", err)
		return true
	}

	return false
}

// Deprecated: no-op
func (d *RedisRateLimiter) Start() {
	// no-op
}

// Deprecated: no-op
func (d *RedisRateLimiter) Stop() {
	// no-op
}

// Deprecated: no-op, always return true
func (d *RedisRateLimiter) IsStart() bool {
	return true
}

// Deprecated: no-op
func (d *RedisRateLimiter) PutBucket(_ string, _ *LeakyBucket) {
	// no-op
}

// Deprecated: no-op, always return nil
func (d *RedisRateLimiter) Bucket(_ string) *LeakyBucket {
	// no-op
	return nil
}

// getKeyCurrentToken returns the currentToken of the key
// getKeyCurrentToken will query redis using GET
// if GET query returns nil / key is unset, getKeyCurrentToken will return 0
// otherwise return the result / error
func (d *RedisRateLimiter) getKeyCurrentToken(ctx context.Context, key string) (currentToken int32, err error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	result, err := d.db.Get(ctx, key).Result()

	if err != nil {
		// if key is unset
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}

		// if legit error
		return 0, err
	}

	if _, err := fmt.Sscanf(result, "%d", &currentToken); err != nil {
		return 0, err
	}

	return currentToken, nil
}

// incrementKeyCounterBy creates a transaction which
// increments the key current token by given value using INCRBY query
// if key is newly created, TTL will be set using EXPIRE query with RedisRateLimiter.duration
func (d *RedisRateLimiter) incrementKeyCounterBy(ctx context.Context, topic string, value int64) error {
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
func (d *RedisRateLimiter) isLegitRedisError(err error) bool {
	return err != nil && !errors.Is(err, redis.Nil)
}

// getKeyFormatted returns topic prepended with RedisRateLimiter.keyPrefix if it's not empty
func (d *RedisRateLimiter) getKeyFormatted(topic string) string {
	if d.keyPrefix != "" {
		return fmt.Sprintf("%s:%s", d.keyPrefix, topic)
	}

	return topic
}
