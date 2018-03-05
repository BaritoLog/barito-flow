package receiver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/BaritoLog/go-boilerplate/httpkit"
	"github.com/BaritoLog/go-boilerplate/testkit"
)

func TestNewContext(t *testing.T) {
	ctx := NewContext()
	testkit.FatalIf(
		t,
		ctx == nil,
		"ctx should be initiate",
	)
}

func TestContext_Init(t *testing.T) {
	ctx := context{}

	err := ctx.Init(Configuration{addr: ":8888"})
	testkit.FatalIfError(t, err)

	server := ctx.server.(*http.Server)
	testkit.FatalIf(
		t,
		server.Addr != ":8888",
		"Addr should be :8888",
	)

	testkit.FatalIf(
		t,
		server.Handler == nil,
		"Handler should be initiate",
	)
}

func TestContext_Run(t *testing.T) {
	ctx := context{
		server: httpkit.DummyServer{
			ErrListAndServer: errkit.Error("error1"),
		},
	}

	err := ctx.Run()
	testkit.FatalIfWrongError(t, err, "error1")
}

func TestContext_ProduceHandler(t *testing.T) {
	ctx := context{}

	// submit to /produce
	req, _ := http.NewRequest(
		"POST",
		"/str/18/st/1/fw/1/cl/10/produce/kafka-dummy-topic",
		strings.NewReader("expected body"),
	)
	rec := httptest.NewRecorder()

	http.HandlerFunc(ctx.produceHandler).ServeHTTP(rec, req)
	testkit.FatalIfWrongHttpCode(t, rec, http.StatusOK)
}
