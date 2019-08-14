package flow

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/BaritoLog/barito-flow/proto"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/instru"
)

func TestElasticStore_CreateIndexError(t *testing.T) {
	defer instru.Flush()

	timber := *pb.SampleTimberProto()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusNotFound,
		CreateAPIStatus: http.StatusInternalServerError,
		PostAPIStatus:   http.StatusBadRequest,
	})
	defer ts.Close()

	retrier := mockElasticRetrier()
	esConfig := NewEsConfig("SingleInsert", 100, time.Duration(1000), false)
	client, err := NewElastic(retrier, esConfig, ts.URL)
	FatalIfError(t, err)

	err = client.Store(context.Background(), timber)
	FatalIfWrongError(t, err, "elastic: Error 500 (Internal Server Error)")
	FatalIf(t, instru.GetEventCount("es_create_index", "fail") != 1, "wrong total es_create_index.fail event")
}

func TestElasticStore_CreateindexSuccess(t *testing.T) {
	defer instru.Flush()

	timber := *pb.SampleTimberProto()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusNotFound,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	retrier := mockElasticRetrier()
	esConfig := NewEsConfig("SingleInsert", 100, time.Duration(1000), false)
	client, err := NewElastic(retrier, esConfig, ts.URL)
	FatalIfError(t, err)

	appSecret := timber.GetContext().GetAppSecret()

	err = client.Store(context.Background(), timber)
	FatalIfError(t, err)
	FatalIf(t, instru.GetEventCount("es_create_index", "success") != 1, "wrong es_store.total success event")
	FatalIf(t, instru.GetEventCount(fmt.Sprintf("%s_es_store", appSecret), "success") != 1, "wrong total es_store.success event")
}

func TestElasticStoreman_store_SaveError(t *testing.T) {
	defer instru.Flush()

	timber := *pb.SampleTimberProto()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusBadRequest,
	})
	defer ts.Close()

	retrier := mockElasticRetrier()
	esConfig := NewEsConfig("SingleInsert", 100, time.Duration(1000), false)
	client, err := NewElastic(retrier, esConfig, ts.URL)
	FatalIfError(t, err)

	appSecret := timber.GetContext().GetAppSecret()

	err = client.Store(context.Background(), timber)
	FatalIfWrongError(t, err, "elastic: Error 400 (Bad Request)")
	FatalIf(t, instru.GetEventCount(fmt.Sprintf("%s_es_store", appSecret), "fail") != 1, "wrong total fail event")
}
