package receiver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/imantung/go-boilerplate/testkit"
)

func TestContext_Init(t *testing.T) {
	ctx := NewContext()

	// init
	conf := Configuration{addr: ":8888"}
	err := ctx.Init(conf)
	testkit.FatalIfError(t, err)
	testkit.FatalIf(
		t,
		ctx.Configuration() != conf,
		"conf should be initiate",
	)

}

func TestContext_ProduceHandler(t *testing.T) {
	ctx := context{}

	// submit to /produce
	req, _ := http.NewRequest(
		"POST",
		"/produce",
		strings.NewReader("expected body"),
	)
	rec := httptest.NewRecorder()

	http.HandlerFunc(ctx.ProduceHandler).ServeHTTP(rec, req)
	testkit.FatalIfWrongHttpCode(t, rec, http.StatusOK)

}
