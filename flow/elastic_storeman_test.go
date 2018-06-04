package flow

import (
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/instru"
	"github.com/olivere/elastic"
)

func TestElasticStoreman_store_CreateIndexError(t *testing.T) {
	defer instru.Flush()

	timber := NewTimber()

	ts := ElasticTestServer(http.StatusNotFound, http.StatusInternalServerError, http.StatusOK)
	defer ts.Close()

	client, err := elastic.NewClient(
		elastic.SetURL(ts.URL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)
	FatalIfError(t, err)

	storeman := NewElasticStoreman(client)
	err = storeman.Store(timber)
	FatalIfWrongError(t, err, "elastic: Error 500 (Internal Server Error)")
	FatalIf(t, instru.GetEventCount("es_create_index", "fail") != 1, "wrong total es_create_index.fail event")
}

func TestElasticStoreman_store_CreateindexSuccess(t *testing.T) {
	defer instru.Flush()

	timber := NewTimber()

	ts := ElasticTestServer(http.StatusNotFound, http.StatusOK, http.StatusOK)
	defer ts.Close()

	client, err := elastic.NewClient(
		elastic.SetURL(ts.URL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)
	FatalIfError(t, err)

	storeman := NewElasticStoreman(client)
	err = storeman.Store(timber)
	FatalIfError(t, err)
	FatalIf(t, instru.GetEventCount("es_create_index", "success") != 1, "wrong es_store.total success event")
	FatalIf(t, instru.GetEventCount("es_store", "success") != 1, "wrong total es_store.success event")
}

func TestElasticStoreman_store_SaveError(t *testing.T) {
	defer instru.Flush()

	timber := NewTimber()

	ts := ElasticTestServer(http.StatusOK, http.StatusOK, http.StatusBadRequest)
	defer ts.Close()

	client, err := elastic.NewClient(
		elastic.SetURL(ts.URL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)
	FatalIfError(t, err)

	storeman := NewElasticStoreman(client)
	err = storeman.Store(timber)
	FatalIfWrongError(t, err, "elastic: Error 400 (Bad Request)")
	FatalIf(t, instru.GetEventCount("es_store", "fail") != 1, "wrong total fail event")
}

func ElasticTestServer(existStatusCode, createStatusCode, sendStatusCode int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" { // check if index exist
			w.WriteHeader(existStatusCode)
		} else if r.Method == "PUT" { // create index
			w.WriteHeader(createStatusCode)
			w.Write([]byte(`{}`))
		} else if r.Method == "POST" { // post message
			w.WriteHeader(sendStatusCode)
			w.Write([]byte(`{}`))
		}
	}))
}
