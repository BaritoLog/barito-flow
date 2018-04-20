package river

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestReceiverUpstream_New(t *testing.T) {
	upstream, err := NewReceiverUpstream(ReceiverUpstreamConfig{Addr: ":8080"})
	FatalIfError(t, err)

	recevier, ok := upstream.(*receiverUpstream)
	FatalIf(t, !ok, "upstream must be ReceiverUpstream")
	FatalIf(t, recevier.addr == "", "must setup addr in receiver")
	FatalIf(t, recevier.errCh == nil, "must setup error channel in receiver")
	FatalIf(t, recevier.timberCh == nil, "must setup timber channel in receiver")
}

func TestReceiverUpstream_New_WrongParameter(t *testing.T) {
	_, err := NewReceiverUpstream("meh")
	FatalIfWrongError(t, err, "Parameter must be ReceiverUpstreamConfig")
}

func TestReceiverUpstream_ProduceHandler_Success(t *testing.T) {
	upstream := &receiverUpstream{
		addr:     ":8080",
		timberCh: make(chan Timber),
		errCh:    make(chan error),
	}

	timber := Timber{
		Message: "some message",
	}
	b, _ := json.Marshal(timber)
	req, _ := http.NewRequest("POST", "/topic", bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(upstream.ServeHTTP)
	handler.ServeHTTP(rr, req)

	FatalIfWrongHttpCode(t, rr, 200)

	got := <-upstream.TimberChannel()
	FatalIf(t, got.Message != "some message", "Got wrong message: %s", got.Message)
}

func TestReceiverUpstream_SetErrorChannel(t *testing.T) {
	errCh := make(chan error)
	receiver := receiverUpstream{}

	receiver.SetErrorChannel(errCh)
	FatalIf(t, errCh != receiver.ErrorChannel(), "SetErrorChannel is not working")

}
