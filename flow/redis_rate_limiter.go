package flow

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"syscall"
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

// WithFallbackToLocal enables fallback to given limiter whenever redis is stopped
func WithFallbackToLocal(limiter Limiter) DistributedRateLimiterOpts {
	return func(d *RedisRateLimiter) {
		d.localLimiter = limiter
	}
}

// WithMutex is used whenever DistributedRateLimiter is used inside a goroutine to avoid race condition
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

	localLimiter Limiter
	isUsingLocal bool
}

// NewRedisRateLimiter creates *DistributedRateLimiter
func NewRedisRateLimiter(db *redis.Client, opts ...DistributedRateLimiterOpts) *RedisRateLimiter {
	d := &RedisRateLimiter{
		db:        db,
		timeout:   defaultTimeout,
		duration:  defaultDuration,
		keyPrefix: defaultKeyPrefix,
		localLimiter: LimiterFunc(func(_ string, _ int, _ int32) bool {
			// not limit anything
			return false
		}),
	}

	for _, opt := range opts {
		opt(d)
	}

	db.Options().OnConnect = d.onConnect
	return d
}

// IsHitLimit implements best practice from redis official website with some optimization
// https://redis.com/redis-best-practices/basic-rate-limiting/
func (d *RedisRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	if d.Mutex != nil {
		d.Lock()
		defer d.Unlock()
	}

	if d.isUsingLocal {
		return d.localLimiter.IsHitLimit(topic, count, maxTokenIfNotExist)
	}

	key := d.getKeyFormatted(topic)
	currentToken, err := d.incrBy(context.Background(), key, count)
	if d.onErr(err) {
		return true
	}

	if currentToken > (maxTokenIfNotExist * int32(d.duration.Seconds())) {
		log.Debugf("key: %v is reaching limit", key)
		return true
	}

	log.Debugf("current token: %v", currentToken)
	if int(currentToken) != count {
		return false
	}

	log.Debugf("current token == count, means the key is newly created, set expire NX")
	if err := d.expireNx(context.Background(), key); d.onErr(err) {
		return true
	}

	return false
}

// Start starts the DistributedRateLimiter.localLimiter
func (d *RedisRateLimiter) Start() {
	d.callLocalStartFunction()
}

// Stop stops the DistributedRateLimiter.localLimiter
func (d *RedisRateLimiter) Stop() {
	d.callLocalStopFunction()
}

// IsStart checks whether the DistributedRateLimiter.localLimiter is started
func (d *RedisRateLimiter) IsStart() bool {
	return d.callLocalIsStartFunction()
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

// incrBy increments the key current token by given value using INCRBY query
func (d *RedisRateLimiter) incrBy(ctx context.Context, key string, value int) (int32, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	token, err := d.db.IncrBy(ctx, key, int64(value)).Result()
	if d.isLegitRedisError(err) {
		return 0, err
	}

	return int32(token), nil
}

// expireNx sets TTL using EXPIRE query with DistributedRateLimiter.duration
// only if the key is newly created
func (d *RedisRateLimiter) expireNx(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	if err := d.db.ExpireNX(ctx, key, d.duration).Err(); d.isLegitRedisError(err) {
		return err
	}

	return nil
}

// isLegitRedisError checks whether given err is truly redis error, and not Nil returned by redis
func (d *RedisRateLimiter) isLegitRedisError(err error) bool {
	return err != nil && !errors.Is(err, redis.Nil)
}

// isRedisConnectionRefused checks whether given err is due to the connection error
func (d *RedisRateLimiter) isRedisConnectionRefused(err error) bool {
	return err != nil && errors.Is(err, syscall.ECONNREFUSED)
}

// getKeyFormatted returns topic prepended with DistributedRateLimiter.keyPrefix if it's not empty
func (d *RedisRateLimiter) getKeyFormatted(topic string) string {
	if d.keyPrefix != "" {
		return fmt.Sprintf("%s:%s", d.keyPrefix, topic)
	}

	return topic
}

// onConnect is a hook that will be called whenever redis connection is recovered
func (d *RedisRateLimiter) onConnect(_ context.Context, _ *redis.Conn) error {
	log.Debug("(re)connected to redis")
	d.isUsingLocal = false
	return nil
}

// onErr is a high-level error handling function
// if given err is connection refused, DistributedRateLimiter.isUsingLocal will be set to true
// returns true if DistributedRateLimiter.isLegitRedisError(err) is true
func (d *RedisRateLimiter) onErr(err error) bool {
	if !d.isLegitRedisError(err) {
		return false
	}

	if d.isRedisConnectionRefused(err) {
		log.Debugf("redis disconnected, start using local limiter")
		d.isUsingLocal = true
	}

	log.Errorf("unexpected error: %v", err)
	return true
}

// isLocalRateLimiter checks whether DistributedRateLimiter.localLimiter is implementing RateLimiter interface
func (d *RedisRateLimiter) isLocalRateLimiter() (RateLimiter, bool) {
	if d.localLimiter != nil {
		rl, ok := d.localLimiter.(RateLimiter)
		return rl, ok
	}

	return nil, false
}

// callLocalStartFunction calls DistributedRateLimiter.localLimiter.Start() if set
func (d *RedisRateLimiter) callLocalStartFunction() {
	rl, ok := d.isLocalRateLimiter()
	if ok {
		rl.Start()
	}
}

// callLocalStopFunction calls DistributedRateLimiter.localLimiter.Stop() if set
func (d *RedisRateLimiter) callLocalStopFunction() {
	rl, ok := d.isLocalRateLimiter()
	if ok {
		rl.Stop()
	}
}

// callLocalIsStartFunction calls DistributedRateLimiter.localLimiter.IsStart() if set
// otherwise return true
func (d *RedisRateLimiter) callLocalIsStartFunction() bool {
	rl, ok := d.isLocalRateLimiter()
	if ok {
		return rl.IsStart()
	}

	return true
}
