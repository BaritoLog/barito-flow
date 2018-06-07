package flow

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestHttpAgent_ServeHTTP(t *testing.T) {

	agent := NewHttpAgent("", func(timber Timber) error {
		return nil
	}, 100)

	body := strings.NewReader(`body`)

	req, _ := http.NewRequest("POST", "/", body)
	rr := HttpRecord(agent.ServeHTTP, req)

	FatalIfWrongHttpCode(t, rr, http.StatusOK)
}

func TestHttpAgent_ServeHTTP_StoreError(t *testing.T) {
	agent := NewHttpAgent("", func(timber Timber) error {
		return fmt.Errorf("some error")
	}, 100)

	body := strings.NewReader(`body`)

	req, _ := http.NewRequest("POST", "/", body)
	rr := HttpRecord(agent.ServeHTTP, req)

	FatalIfWrongHttpCode(t, rr, http.StatusBadGateway)
}

func TestHttpAgent_Start(t *testing.T) {
	agent := NewHttpAgent(":65500", func(timber Timber) error {
		return nil
	}, 100)

	go agent.Start()
	defer agent.Close()

	buf := bytes.NewBufferString(`{}`)
	resp, err := http.Post("http://localhost:65500", "application/json", buf)

	FatalIfError(t, err)
	FatalIf(t, resp.StatusCode != 200, "wrong status code")

	agent.Close()
}

func TestHttpAgent_HitMaxTPS(t *testing.T) {
	maxTps := 10
	agent := NewHttpAgent(":65501", func(timber Timber) error {
		return nil
	}, maxTps)
	go agent.Start()
	defer agent.Close()

	for i := 0; i < maxTps; i++ {
		http.Post("http://localhost:65501", "application/json", bytes.NewBufferString(`{}`))
	}

	resp, err := http.Post("http://localhost:65501", "application/json", bytes.NewBufferString(`{}`))
	FatalIfError(t, err)
	FatalIf(t, resp.StatusCode != 509, "wrong status code")
}

func TestHttp_Agent_RefillBucket(t *testing.T) {
	maxTps := 10
	agent := NewHttpAgent(":65502", func(timber Timber) error {
		return nil
	}, maxTps)
	go agent.Start()
	defer agent.Close()

	for i := 0; i < maxTps; i++ {
		http.Post("http://localhost:65502", "application/json", bytes.NewBufferString(`{}`))
	}

	timekit.Sleep("1s")

	resp, err := http.Post("http://localhost:65502", "application/json", bytes.NewBufferString(`{}`))
	FatalIfError(t, err)
	FatalIf(t, resp.StatusCode != 200, "wrong status code")

}
