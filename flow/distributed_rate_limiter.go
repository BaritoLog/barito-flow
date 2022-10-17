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

// WithKeyPrefix enables separation of key among redis usage
func WithKeyPrefix(prefix string) DistributedRateLimiterOpts {
	return func(d *DistributedRateLimiter) {
		d.keyPrefix = strings.TrimSpace(prefix)
	}
}

func WithFallbackToLocal(limiter Limiter) DistributedRateLimiterOpts {
	return func(d *DistributedRateLimiter) {
		d.localLimiter = limiter
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
	db        *redis.Client
	timeout   time.Duration
	duration  time.Duration
	keyPrefix string

	localLimiter Limiter
	isUsingLocal bool
}

// NewDistributedRateLimiter creates *DistributedRateLimiter
func NewDistributedRateLimiter(db *redis.Client, opts ...DistributedRateLimiterOpts) *DistributedRateLimiter {
	d := &DistributedRateLimiter{
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

// IsHitLimit implements best practice from redis official website
// https://redis.com/redis-best-practices/basic-rate-limiting/
func (d *DistributedRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	if d.Mutex != nil {
		d.Lock()
		defer d.Unlock()
	}

	if d.isUsingLocal {
		return d.localLimiter.IsHitLimit(topic, count, maxTokenIfNotExist)
	}

	key := d.getKeyFormatted(topic)
	currentToken, err := d.getKeyCurrentToken(context.Background(), key)
	if d.onErr(err) {
		return true
	}

	if (currentToken + int32(count)) > (maxTokenIfNotExist * int32(d.duration.Seconds())) {
		log.Debugf("key: %v is reaching limit", key)
		return true
	}

	log.Debugf("current token: %v", currentToken)
	if err := d.incrementKeyCounterBy(context.Background(), key, int64(count)); d.onErr(err) {
		return true
	}

	return false
}

// Start starts the DistributedRateLimiter.localLimiter
func (d *DistributedRateLimiter) Start() {
	d.callLocalStartFunction()
}

// Stop stops the DistributedRateLimiter.localLimiter
func (d *DistributedRateLimiter) Stop() {
	d.callLocalStopFunction()
}

// IsStart checks whether the DistributedRateLimiter.localLimiter is started
func (d *DistributedRateLimiter) IsStart() bool {
	return d.callLocalIsStartFunction()
}

// Deprecated: no-op
func (d *DistributedRateLimiter) PutBucket(_ string, _ *LeakyBucket) {
	// no-op
}

// Deprecated: no-op, always return nil
func (d *DistributedRateLimiter) Bucket(_ string) *LeakyBucket {
	// no-op
	return nil
}

// getKeyCurrentToken returns the currentToken of the key
// getKeyCurrentToken will query redis using GET
// if GET query returns nil / key is unset, getKeyCurrentToken will return 0
// otherwise return the result / error
func (d *DistributedRateLimiter) getKeyCurrentToken(ctx context.Context, key string) (currentToken int32, err error) {
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
// if key is newly created, TTL will be set using EXPIRE query with DistributedRateLimiter.duration
func (d *DistributedRateLimiter) incrementKeyCounterBy(ctx context.Context, topic string, value int64) error {
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

// isRedisConnectionRefused checks whether given err is due to the connection error
func (d *DistributedRateLimiter) isRedisConnectionRefused(err error) bool {
	return err != nil && errors.Is(err, syscall.ECONNREFUSED)
}

// getKeyFormatted returns topic prepended with DistributedRateLimiter.keyPrefix if it's not empty
func (d *DistributedRateLimiter) getKeyFormatted(topic string) string {
	if d.keyPrefix != "" {
		return fmt.Sprintf("%s:%s", d.keyPrefix, topic)
	}

	return topic
}

// onConnect is a hook that will be called whenever redis connection is recovered
func (d *DistributedRateLimiter) onConnect(_ context.Context, _ *redis.Conn) error {
	log.Debug("(re)connected to redis")
	d.isUsingLocal = false
	return nil
}

// onErr is a high-level error handling function
// if given err is connection refused, DistributedRateLimiter.isUsingLocal will be set to true
// returns true if DistributedRateLimiter.isLegitRedisError(err) is true
func (d *DistributedRateLimiter) onErr(err error) bool {
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
func (d *DistributedRateLimiter) isLocalRateLimiter() (RateLimiter, bool) {
	if d.localLimiter != nil {
		rl, ok := d.localLimiter.(RateLimiter)
		return rl, ok
	}

	return nil, false
}

// callLocalStartFunction calls DistributedRateLimiter.localLimiter.Start() if set
func (d *DistributedRateLimiter) callLocalStartFunction() {
	rl, ok := d.isLocalRateLimiter()
	if ok {
		rl.Start()
	}
}

// callLocalStopFunction calls DistributedRateLimiter.localLimiter.Stop() if set
func (d *DistributedRateLimiter) callLocalStopFunction() {
	rl, ok := d.isLocalRateLimiter()
	if ok {
		rl.Stop()
	}
}

// callLocalIsStartFunction calls DistributedRateLimiter.localLimiter.IsStart() if set
// otherwise return true
func (d *DistributedRateLimiter) callLocalIsStartFunction() bool {
	rl, ok := d.isLocalRateLimiter()
	if ok {
		return rl.IsStart()
	}

	return true
}
