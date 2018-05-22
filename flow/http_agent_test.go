package flow

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/BaritoLog/barito-flow/river"
	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestHttpAgent_ServeHTTP(t *testing.T) {
	agent := &HttpAgent{
		Store: func(timber river.Timber) error {
			return nil
		},
	}

	body := strings.NewReader(`body`)

	req, _ := http.NewRequest("POST", "/", body)
	rr := HttpRecord(agent.ServeHTTP, req)

	FatalIfWrongHttpCode(t, rr, http.StatusOK)
}

func TestHttpAgent_ServeHTTP_StoreError(t *testing.T) {
	agent := &HttpAgent{
		Store: func(timber river.Timber) error {
			return fmt.Errorf("some error")
		},
	}

	body := strings.NewReader(`body`)

	req, _ := http.NewRequest("POST", "/", body)
	rr := HttpRecord(agent.ServeHTTP, req)

	FatalIfWrongHttpCode(t, rr, http.StatusBadGateway)
}

func TestHttpAgent_Start(t *testing.T) {

	agent := &HttpAgent{
		Address: ":65500",
		Store: func(timber river.Timber) error {
			return nil
		},
	}

	go agent.Start()

	buf := bytes.NewBufferString(`{}`)
	resp, err := http.Post("http://localhost:65500", "application/json", buf)

	FatalIfError(t, err)
	FatalIf(t, resp.StatusCode != 200, "wrong ")

	agent.Close()
}
