package instru

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestWebCallback(t *testing.T) {
	expectedBody := []byte(`{"evaluations":{"barito-flow/flow/elastic.go#store":{"count":12,"avg":5000,"sum":0,"max":10000,"min":1000,"recent":1000}},"counters":{"elastic":{"Total":21,"Events":{"error":1,"success":19}}}}`)
	var gotBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = ioutil.ReadAll(r.Body)
	}))
	defer ts.Close()

	instr := &instrumentation{}
	err := json.Unmarshal(expectedBody, instr)
	FatalIfError(t, err)

	callback := NewWebCallback(ts.URL)
	err = callback.OnCallback(instr)

	FatalIfError(t, err)
	FatalIf(t, bytes.Compare(gotBody, expectedBody) != 0, "got wrong body")
}

func TestWebCallback_ClientError(t *testing.T) {
	callback := NewWebCallback("wrong-url")
	err := callback.OnCallback(NewInstrumentation())

	FatalIfWrongError(t, err, `Post wrong-url: unsupported protocol scheme ""`)
}

func TestWebCallback_StatusCodeNotOK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	callback := NewWebCallback(ts.URL)
	err := callback.OnCallback(NewInstrumentation())
	FatalIfWrongError(t, err, "Got status code 500")
}
