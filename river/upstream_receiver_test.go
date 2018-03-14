package river

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestReceiverUpstream_WrongParameter(t *testing.T) {
	_, err := NewReceiverUpstream("meh")
	FatalIfWrongError(t, err, "Parameter must be ReceiverUpstreamConfig")
}

func TestReceiverUpstream_ProduceHandler_Success(t *testing.T) {
	upstream := &receiverUpstream{
		addr:     ":8080",
		timberCh: make(chan Timber),
		errCh:    make(chan error),
	}

	// submit to /produce
	req, _ := http.NewRequest(
		"POST",
		"/str/18/st/1/fw/1/cl/10/produce/kafka-dummy-topic",
		strings.NewReader("expected body"),
	)
	rec := httptest.NewRecorder()

	go upstream.router().ServeHTTP(rec, req)

	timekit.Sleep("1ms")

	FatalIfWrongHttpCode(t, rec, http.StatusOK)
}
