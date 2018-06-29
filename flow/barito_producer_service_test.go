package flow

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestHttpAgent_ServeHTTP(t *testing.T) {

	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndSucceed()

	agent := NewBaritoProducerService("", producer, 100)
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`{}`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusOK)
}

func TestHttpAgent_ServeHTTP_StoreError(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndFail(fmt.Errorf("some error"))

	agent := NewBaritoProducerService("", producer, 100)
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`{}`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadGateway)
}

func TestHttpAgent_Start(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndSucceed()

	agent := NewBaritoProducerService(":65500", producer, 100)

	go agent.Start()
	defer agent.Close()

	resp, err := http.Post("http://localhost:65500", "application/json", strings.NewReader(`{}`))

	FatalIfError(t, err)
	FatalIfWrongResponseStatus(t, resp, 200)

	agent.Close()
}

func TestHttpAgent_HitMaxTPS(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())

	maxTps := 10
	agent := NewBaritoProducerService(":65501", producer, maxTps)
	go agent.Start()
	defer agent.Close()

	for i := 0; i < maxTps; i++ {
		producer.ExpectSendMessageAndSucceed()
		http.Post("http://localhost:65501", "application/json", strings.NewReader(`{}`))
	}

	resp, err := http.Post("http://localhost:65501", "application/json", strings.NewReader(`{}`))
	FatalIfError(t, err)
	FatalIfWrongResponseStatus(t, resp, 509)
}

func TestHttpAgent_RefillBucket(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())

	maxTps := 10
	agent := NewBaritoProducerService(":65502", producer, maxTps)
	go agent.Start()
	defer agent.Close()

	for i := 0; i < maxTps; i++ {
		producer.ExpectSendMessageAndSucceed()
		http.Post("http://localhost:65502", "application/json", strings.NewReader(`{}`))
	}

	timekit.Sleep("1s")

	producer.ExpectSendMessageAndSucceed()
	resp, err := http.Post("http://localhost:65502", "application/json", strings.NewReader(`{}`))
	FatalIfError(t, err)
	FatalIfWrongResponseStatus(t, resp, http.StatusOK)
}

func TestHttpAgent_OnBadRequest(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())

	agent := NewBaritoProducerService("", producer, 100)
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`invalid-body`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadRequest)

}
