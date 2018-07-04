package flow

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/BaritoLog/go-boilerplate/saramatestkit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestHttpAgent_ServeHTTP(t *testing.T) {

	var topic string

	dummy := saramatestkit.NewSyncProducer()
	dummy.SendMessageFunc = func(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
		topic = msg.Topic
		return
	}

	agent := NewBaritoProducerService("", dummy, 100, "_logs")
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/",
		strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusOK)
	FatalIf(t, topic != "some_topic_logs", "produce to wrong kafka topic")
}

func TestHttpAgent_ServeHTTP_StoreError(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndFail(fmt.Errorf("some error"))

	agent := NewBaritoProducerService("", producer, 100, "_logs")
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadGateway)
}

func TestHttpAgent_Start(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndSucceed()

	agent := NewBaritoProducerService(":65500", producer, 100, "_logs")

	go agent.Start()
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, 200)
}

func TestHttpAgent_HitMaxTPS(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())

	maxTps := 10
	agent := &baritoProducerService{
		Address:     ":8999",
		MaxTps:      maxTps,
		Producer:    producer,
		tps:         maxTps,
		TopicSuffix: "_logs",
	}
	go agent.Start()
	defer agent.Close()

	for i := 0; i < maxTps; i++ {
		producer.ExpectSendMessageAndSucceed()
		req, _ := http.NewRequest("POST", "/", strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}}`))
		RecordResponse(agent.ServeHTTP, req)
		timekit.Sleep("1ms")
	}

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	resp := RecordResponse(agent.ServeHTTP, req)
	FatalIfWrongResponseStatus(t, resp, 509)
}

func TestHttpAgent_RefillBucket(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())

	maxTps := 10
	agent := NewBaritoProducerService(":65502", producer, maxTps, "_logs")
	go agent.Start()
	defer agent.Close()

	for i := 0; i < maxTps; i++ {
		producer.ExpectSendMessageAndSucceed()
		req, _ := http.NewRequest("POST", "/", strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}}`))
		RecordResponse(agent.ServeHTTP, req)
		timekit.Sleep("1ms")
	}

	timekit.Sleep("1s")

	producer.ExpectSendMessageAndSucceed()
	req, _ := http.NewRequest("POST", "/", strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	resp := RecordResponse(agent.ServeHTTP, req)
	FatalIfWrongResponseStatus(t, resp, http.StatusOK)
}

func TestHttpAgent_OnBadRequest(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())

	agent := NewBaritoProducerService("", producer, 100, "_logs")
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`invalid-body`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadRequest)

}
