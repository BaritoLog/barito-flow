package flow

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// GubernatorRateLimiter is a RateLimiter implementation
// which depends on Redis as a remote storage
type GubernatorRateLimiter struct {
	prefix        string
	gubernatorURL string
	httpClient    *http.Client
}

// NewGubernatorRateLimiter creates *GubernatorRateLimiter
func NewGubernatorRateLimiter(gubernatorURL string, prefix string, httpClient *http.Client) *GubernatorRateLimiter {
	g := &GubernatorRateLimiter{
		prefix:        prefix,
		gubernatorURL: gubernatorURL,
		httpClient:    httpClient,
	}

	return g
}

func (g *GubernatorRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	limit := maxTokenIfNotExist * 10
	payload := fmt.Sprintf(
		`{ "requests":[ { "name": "requests_per_sec", "unique_key": "%s:%s", "hits": %d, "duration": 10000, "limit": %d } ] }`,
		g.prefix,
		topic,
		count,
		limit,
	)
	resp, err := g.httpClient.Post(g.gubernatorURL, "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		return true
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return true
	}

	return strings.Contains(string(body), "OVER_LIMIT")
}

// Deprecated: no-op
func (d *GubernatorRateLimiter) Start() {
	// no-op
}

// Deprecated: no-op
func (d *GubernatorRateLimiter) Stop() {
	// no-op
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
