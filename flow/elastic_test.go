package flow

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/instru"
	pb "github.com/bentol/barito-proto/producer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const (
	BARITO_DEFAULT_USERNAME = ""
	BARITO_DEFAULT_PASSWORD = ""
)

func TestElasticStore_CreateIndexError(t *testing.T) {
	resetPrometheusMetrics()
	defer instru.Flush()

	timber := *pb.SampleTimberProto()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusNotFound,
		CreateAPIStatus: http.StatusInternalServerError,
		PostAPIStatus:   http.StatusBadRequest,
	})
	defer ts.Close()

	retrier := mockElasticRetrier()
	esConfig := NewEsConfig("SingleInsert", 100, time.Duration(1000), false, "")
	client, err := NewElastic(retrier, esConfig, []string{ts.URL}, BARITO_DEFAULT_USERNAME, BARITO_DEFAULT_PASSWORD, nil)
	FatalIfError(t, err)

	// Run Store in goroutine since it will loop indefinitely
	done := make(chan error, 1)
	go func() {
		err := client.Store(context.Background(), timber)
		done <- err
	}()

	// Wait for the first index creation failure to be recorded
	// The Store method will keep retrying, but we just want to verify
	// that the index creation failure is properly recorded
	for i := 0; i < 50; i++ { // Wait up to 5 seconds
		if instru.GetEventCount("es_create_index", "fail") >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify that we got at least one create index failure
	FatalIf(t, instru.GetEventCount("es_create_index", "fail") < 1, "should have at least one es_create_index.fail event")
}

func TestElasticStore_CreateindexSuccess(t *testing.T) {
	resetPrometheusMetrics()
	defer instru.Flush()

	timber := *pb.SampleTimberProto()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusNotFound,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	retrier := mockElasticRetrier()
	esConfig := NewEsConfig("SingleInsert", 100, time.Duration(1000), false, "")
	client, err := NewElastic(retrier, esConfig, []string{ts.URL}, BARITO_DEFAULT_USERNAME, BARITO_DEFAULT_PASSWORD, nil)
	FatalIfError(t, err)

	appSecret := timber.GetContext().GetAppSecret()

	err = client.Store(context.Background(), timber)
	FatalIfError(t, err)
	FatalIf(t, instru.GetEventCount("es_create_index", "success") != 1, "wrong es_store.total success event")
	FatalIf(t, instru.GetEventCount(fmt.Sprintf("%s_es_store", appSecret), "success") != 1, "wrong total es_store.success event")
}

func TestElasticStoreman_store_SaveError(t *testing.T) {
	resetPrometheusMetrics()
	defer instru.Flush()

	timber := *pb.SampleTimberProto()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusBadRequest,
	})
	defer ts.Close()

	retrier := mockElasticRetrier()
	esConfig := NewEsConfig("SingleInsert", 100, time.Duration(1000), false, "")
	client, err := NewElastic(retrier, esConfig, []string{ts.URL}, BARITO_DEFAULT_USERNAME, BARITO_DEFAULT_PASSWORD, nil)
	FatalIfError(t, err)

	appSecret := timber.GetContext().GetAppSecret()

	err = client.Store(context.Background(), timber)
	FatalIfWrongError(t, err, "elastic: Error 400 (Bad Request)")
	FatalIf(t, instru.GetEventCount(fmt.Sprintf("%s_es_store", appSecret), "fail") != 1, "wrong total fail event")
}

func TestElasticStore_ExportMetrics(t *testing.T) {
	resetPrometheusMetrics()
	defer instru.Flush()

	timber := *pb.SampleTimberProto()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
		ResponseBody: []byte(`
			{
				"took":2,
				"errors":true,
				"items":[{
					"index":{
						"_index":"index-one-2020.12.31",
						"_type":"tweet",
						"_id":"1",
						"_version":3,
						"status":200
					}
				},{
					"index":{
						"_index":"index-one-2020.12.31",
						"_type":"tweet",
						"_id":"2",
						"_version":3,
						"status":200
					}
				},{
					"delete":{
						"_index":"index2",
						"_type":"tweet",
						"_id":"1",
						"_version":4,
						"status":400,
						"found":true,
						"error": {
							"type": "type",
							"reason": "reason"
						}
					}
				}]
			}
		`),
	})
	defer ts.Close()

	retrier := mockElasticRetrier()
	esConfig := NewEsConfig("BulkProcessor", 100, time.Duration(1000), false, "")
	client, err := NewElastic(retrier, esConfig, []string{ts.URL}, BARITO_DEFAULT_USERNAME, BARITO_DEFAULT_PASSWORD, nil)
	FatalIfError(t, err)

	err = client.Store(context.Background(), timber)
	FatalIfError(t, err)

	time.Sleep(2 * time.Second)
	expected := `
		# HELP barito_consumer_log_stored_total Number log stored to ES
		# TYPE barito_consumer_log_stored_total counter
		barito_consumer_log_stored_total{error="",index="index-one",result="200",status=""} 2
		barito_consumer_log_stored_total{error="undefined_error",index="index2",result="400",status=""} 1
	`
	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_consumer_log_stored_total"))
}
