package cmds

import (
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

	instru.Count("es_store").Event("success")

	callback := NewMetricMarketCallback(ts.URL, "token")
	err := callback.OnCallback(instru.DefaultInstrumentation)

	FatalIfError(t, err)
	FatalIf(t, string(reqBody) != `{"new_log_count":1,"token":"token"}`, "wrong request body")
	FatalIf(t, instru.GetEventCount("es_store", "success") != 0, "instrumentation must flushed")
}

func TestMetricMarketCallback_ClientError(t *testing.T) {
	callback := NewMetricMarketCallback("wrong-formatted-url", "token")

	err := callback.OnCallback(instru.DefaultInstrumentation)
	FatalIfWrongError(t, err, `Post wrong-formatted-url: unsupported protocol scheme ""`)
}

func TestMetricMarketCallback_ReturnNotOkStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	callback := NewMetricMarketCallback(ts.URL, "token")
	err := callback.OnCallback(instru.DefaultInstrumentation)
	FatalIfWrongError(t, err, "Got status code 500")
}
