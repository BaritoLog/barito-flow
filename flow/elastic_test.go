package flow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/instru"
)

func TestElasticStore_CreateIndexError(t *testing.T) {
	defer instru.Flush()

	timber, err := ConvertBytesToTimber([]byte(`{"hello": "world", "_ctx": {"kafka_topic": "some_topic","kafka_partition": 3,"kafka_replication_factor": 1,"es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	FatalIfError(t, err)

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusNotFound,
		CreateAPIStatus: http.StatusInternalServerError,
		PostAPIStatus:   http.StatusBadRequest,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	err = elasticStore(client, context.Background(), timber)
	FatalIfWrongError(t, err, "elastic: Error 500 (Internal Server Error)")
	FatalIf(t, instru.GetEventCount("es_create_index", "fail") != 1, "wrong total es_create_index.fail event")
}

func TestElasticStore_CreateindexSuccess(t *testing.T) {
	defer instru.Flush()

	timber, err := ConvertBytesToTimber([]byte(`{"hello": "world", "_ctx": {"kafka_topic": "some_topic","kafka_partition": 3,"kafka_replication_factor": 1,"es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	FatalIfError(t, err)

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusNotFound,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	err = elasticStore(client, context.Background(), timber)
	FatalIfError(t, err)
	FatalIf(t, instru.GetEventCount("es_create_index", "success") != 1, "wrong es_store.total success event")
	FatalIf(t, instru.GetEventCount("es_store", "success") != 1, "wrong total es_store.success event")
}

func TestElasticStoreman_store_SaveError(t *testing.T) {
	defer instru.Flush()

	timber, err := ConvertBytesToTimber([]byte(`{"hello": "world", "_ctx": {"kafka_topic": "some_topic","kafka_partition": 3,"kafka_replication_factor": 1,"es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	FatalIfError(t, err)

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusBadRequest,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	err = elasticStore(client, context.Background(), timber)
	FatalIfWrongError(t, err, "elastic: Error 400 (Bad Request)")
	FatalIf(t, instru.GetEventCount("es_store", "fail") != 1, "wrong total fail event")
}
