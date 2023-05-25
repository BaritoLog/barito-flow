package flow

import (
	"context"
	"fmt"

	"github.com/mailgun/gubernator/v2"
)

// GubernatorRateLimiter is a RateLimiter implementation
// which depends on Redis as a remote storage
type GubernatorRateLimiter struct {
	gubernatorDaemon   *gubernator.Daemon
	gubernatorInstance *gubernator.V1Instance
	rateLimitInterval  int
}

// NewGubernatorRateLimiter creates *GubernatorRateLimiter
func NewGubernatorRateLimiter(gubernatorDaemon *gubernator.Daemon, rateLimitInterval int) *GubernatorRateLimiter {
	g := &GubernatorRateLimiter{
		gubernatorDaemon:   gubernatorDaemon,
		gubernatorInstance: gubernatorDaemon.V1Server,
		rateLimitInterval:  rateLimitInterval,
	}
	return g
}

func (g *GubernatorRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	resp, err := g.gubernatorInstance.GetRateLimits(
		context.Background(),
		&gubernator.GetRateLimitsReq{
			Requests: []*gubernator.RateLimitReq{
				{
					Name:      "abc",
					UniqueKey: topic,
					Hits:      int64(count),
					Limit:     int64(maxTokenIfNotExist * int32(g.rateLimitInterval)),
					Duration:  gubernator.Second * int64(g.rateLimitInterval),
				},
			},
		},
	)
	if err != nil || len(resp.Responses) == 0 {
		fmt.Println("err", err)
		return true
	}

	return resp.Responses[0].GetStatus() == gubernator.Status_OVER_LIMIT
}

// Deprecated: no-op
func (d *GubernatorRateLimiter) Start() {
	// no-op
}

func (d *GubernatorRateLimiter) Stop() {
	d.gubernatorDaemon.Close()
}

// Deprecated: no-op, always return true
func (d *GubernatorRateLimiter) IsStart() bool {
	return true
}

// Deprecated: no-op
func (d *GubernatorRateLimiter) PutBucket(_ string, _ *LeakyBucket) {
	// no-op
}

// Deprecated: no-op, always return nil
func (d *GubernatorRateLimiter) Bucket(_ string) *LeakyBucket {
	// no-op
	return nil
}
