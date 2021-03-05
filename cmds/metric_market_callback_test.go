package cmds

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/instru"
)

func TestMetricMarketCallback(t *testing.T) {
	var reqBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBody, _ = ioutil.ReadAll(r.Body)
	}))
	appSecret := "some-secret-1234"
	instru.Count(fmt.Sprintf("%s_es_store", appSecret)).Event("success")
	instru.Metric("application_group").Put("app_secrets", []string{"some-secret-1234"})

	callback := NewMetricMarketCallback(ts.URL)
	err := callback.OnCallback(instru.DefaultInstrumentation)

	FatalIfError(t, err)
	FatalIf(t, string(reqBody) != `{"application_groups":[{"token":"some-secret-1234","new_log_count":1}]}`, "wrong request body")
	FatalIf(t, instru.GetEventCount("es_store", "success") != 0, "instrumentation must flushed")
}

func TestMetricMarketCallback_ClientError(t *testing.T) {
	callback := NewMetricMarketCallback("wrong-formatted-url")

	err := callback.OnCallback(instru.DefaultInstrumentation)
	FatalIfWrongError(t, err, `Post "wrong-formatted-url": unsupported protocol scheme ""`)
}

func TestMetricMarketCallback_ReturnNotOkStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	callback := NewMetricMarketCallback(ts.URL)
	err := callback.OnCallback(instru.DefaultInstrumentation)
	FatalIfWrongError(t, err, "Got status code 500")
}
