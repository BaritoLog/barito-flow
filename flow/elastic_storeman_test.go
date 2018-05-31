package flow

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/BaritoLog/barito-flow/river"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/olivere/elastic"
)

func TestElasticStoreman_store_CreateIndexError(t *testing.T) {
	timber := river.NewTimber()

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
}

func TestElasticStoreman_store_CreateindexSuccess(t *testing.T) {
	timber := river.NewTimber()

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
}

func TestElasticStoreman_store_SaveError(t *testing.T) {
	timber := river.NewTimber()

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
