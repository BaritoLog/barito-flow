package flow

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGubernatorRateLimiter(t *testing.T) {
	max := int32(5)

	limiterPrefix := "foo"
	topicName := "abc"

	expectedHits := []int{1, 1, 1, 1, 1, 1, 2}
	gotHits := []int{}

	responses := []string{
		`{"responses":[{"status":"UNDER_LIMIT","limit":"5","remaining":"4","reset_time":"","error":"","metadata":{}}]}`,
		`{"responses":[{"status":"UNDER_LIMIT","limit":"5","remaining":"3","reset_time":"","error":"","metadata":{}}]}`,
		`{"responses":[{"status":"UNDER_LIMIT","limit":"5","remaining":"2","reset_time":"","error":"","metadata":{}}]}`,
		`{"responses":[{"status":"UNDER_LIMIT","limit":"5","remaining":"1","reset_time":"","error":"","metadata":{}}]}`,
		`{"responses":[{"status":"UNDER_LIMIT","limit":"5","remaining":"0","reset_time":"","error":"","metadata":{}}]}`,
		`{"responses":[{"status":"OVER_LIMIT","limit":"5","remaining":"0","reset_time":"","error":"","metadata":{}}]}`,
		`{"responses":[{"status":"UNDER_LIMIT","limit":"5","remaining":"4","reset_time":"","error":"","metadata":{}}]}`,
	}
	responsesIncr := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		gotHits = append(gotHits, int(gjson.GetBytes(body, "requests.0.hits").Int()))
		uniqueKey := gjson.GetBytes(body, "requests.0.unique_key").String()

		assert.Equal(t, limiterPrefix+":"+topicName, uniqueKey)
		w.Write([]byte(responses[responsesIncr]))
		responsesIncr++
	}))

	limiter := NewGubernatorRateLimiter(server.URL, limiterPrefix, http.DefaultClient)

	for i := int32(0); i < max; i++ {
		require.Truef(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", i)
	}
	require.Falsef(t, limiter.IsHitLimit("abc", 1, max), "it should not have token at abc at #6 request")
	require.True(t, limiter.IsHitLimit("abc", 2, max), "it should have token at abc at #7 request")

	require.Equal(t, expectedHits, gotHits)
}
