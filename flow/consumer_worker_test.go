package flow

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func TestKafkaAgent(t *testing.T) {
	messages := make(chan *sarama.ConsumerMessage)
	notifications := make(chan *cluster.Notification)

	expectedNotification := &cluster.Notification{}

	go func() {
		messages <- &sarama.ConsumerMessage{
			Value: []byte(`{"hello": "world", "_ctx": {"kafka_topic": "some_topic"}}`),
		}
		notifications <- expectedNotification
	}()
	timekit.Sleep("1ms")

	var gotTimber Timber
	var gotNotification *cluster.Notification

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	consumer := &dummyKafkaConsumer{
		messages:      messages,
		notifications: notifications,
		errors:        make(chan error),
	}

	agent := NewConsumerWorker(consumer, client)
	agent.OnSuccess(func(timber Timber) { gotTimber = timber })
	agent.OnNotification(func(notification *cluster.Notification) { gotNotification = notification })
	defer agent.Close()

	go agent.Start()
	timekit.Sleep("2ms")

	FatalIf(t, gotTimber["hello"] != "world", "wrong gotTimber[hello]")
}

func TestKafkaAgent_StoreError(t *testing.T) {
	messages := make(chan *sarama.ConsumerMessage)
	go func() {
		messages <- &sarama.ConsumerMessage{
			Value: []byte(`{"hello": "world", "_ctx": {"kafka_topic": "some_topic"}}`),
		}
	}()
	timekit.Sleep("1ms")

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusInternalServerError,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	consumer := &dummyKafkaConsumer{
		messages:      messages,
		notifications: make(chan *cluster.Notification),
		errors:        make(chan error),
	}

	agent := NewConsumerWorker(consumer, client)
	defer agent.Close()

	var err0 error
	agent.OnError(func(err error) { err0 = err })

	go agent.Start()
	timekit.Sleep("2ms")

	FatalIfWrongError(t, err0, string(StoreFailedError))
}

func TestKafkaAgent_KafkaError(t *testing.T) {
	errors := make(chan error)
	go func() {
		errors <- fmt.Errorf("expected kafka error")
	}()
	timekit.Sleep("1ms")

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusInternalServerError,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	consumer := &dummyKafkaConsumer{
		messages:      make(chan *sarama.ConsumerMessage),
		notifications: make(chan *cluster.Notification),
		errors:        errors,
	}

	var err0 error
	agent := NewConsumerWorker(consumer, client)
	agent.OnError(func(err error) { err0 = err })
	defer agent.Close()

	go agent.Start()
	timekit.Sleep("1ms")

	FatalIfWrongError(t, err0, "expected kafka error")
}

func TestKafkaAgent_BadKafkaMessage(t *testing.T) {
	messages := make(chan *sarama.ConsumerMessage)

	go func() {
		messages <- &sarama.ConsumerMessage{
			Value: []byte(`invalid message`),
		}
	}()
	timekit.Sleep("1ms")

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	consumer := &dummyKafkaConsumer{
		messages:      messages,
		notifications: make(chan *cluster.Notification),
		errors:        make(chan error),
	}

	agent := NewConsumerWorker(consumer, client)
	defer agent.Close()

	var err0 error
	agent.OnError(func(err error) { err0 = err })

	go agent.Start()
	timekit.Sleep("1ms")

	FatalIfWrongError(t, err0, string(BadKafkaMessageError))
}
