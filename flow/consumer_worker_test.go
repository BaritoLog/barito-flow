package flow

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/BaritoLog/barito-flow/mock"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/mock/gomock"
)

func TestKafkaAgent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock.NewMockClusterConsumer(ctrl)
	consumer.EXPECT().Messages().AnyTimes().
		Return(sampleMessageChannel(sampleConsumerMessage()))
	consumer.EXPECT().Notifications().
		Return(sampleNotificationChannel())
	consumer.EXPECT().Errors().
		Return(sampleErrorChannel())
	consumer.EXPECT().MarkOffset(gomock.Any(), gomock.Any())
	consumer.EXPECT().Close()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	var gotTimber Timber
	var gotNotification *cluster.Notification

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
			Value: []byte(`{"hello": "world", "_ctx": {"kafka_topic": "some_topic","kafka_partition": 3,"kafka_replication_factor": 1,"es_index_prefix": "some-type","es_document_type": "some-type"}}`),
		}
	}()
	timekit.Sleep("1ms")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock.NewMockClusterConsumer(ctrl)
	consumer.EXPECT().Messages().AnyTimes().
		Return(sampleMessageChannel(sampleConsumerMessage()))
	consumer.EXPECT().Notifications().
		Return(sampleNotificationChannel())
	consumer.EXPECT().Errors().
		Return(sampleErrorChannel())
	consumer.EXPECT().MarkOffset(gomock.Any(), gomock.Any())
	consumer.EXPECT().Close()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusInternalServerError,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	agent := NewConsumerWorker(consumer, client)
	defer agent.Close()

	var err0 error
	agent.OnError(func(err error) { err0 = err })

	go agent.Start()
	timekit.Sleep("2ms")

	FatalIfWrongError(t, err0, string(StoreFailedError))
}

func TestKafkaAgent_KafkaError(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock.NewMockClusterConsumer(ctrl)
	consumer.EXPECT().Messages().AnyTimes().
		Return(sampleMessageChannel())
	consumer.EXPECT().Notifications().
		Return(sampleNotificationChannel())
	consumer.EXPECT().Errors().
		Return(sampleErrorChannel(fmt.Errorf("expected kafka error")))
	consumer.EXPECT().Close()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	var err0 error
	agent := NewConsumerWorker(consumer, client)
	agent.OnError(func(err error) { err0 = err })
	defer agent.Close()

	go agent.Start()
	timekit.Sleep("1ms")

	FatalIfWrongError(t, err0, "expected kafka error")
}

func TestKafkaAgent_BadKafkaMessage(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock.NewMockClusterConsumer(ctrl)
	consumer.EXPECT().Messages().AnyTimes().
		Return(sampleMessageChannel(&sarama.ConsumerMessage{Value: []byte(`invalid message`)}))
	consumer.EXPECT().Notifications().
		Return(sampleNotificationChannel())
	consumer.EXPECT().Errors().
		Return(sampleErrorChannel())
	consumer.EXPECT().MarkOffset(gomock.Any(), gomock.Any())
	consumer.EXPECT().Close()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	client, err := elasticNewClient(ts.URL)
	FatalIfError(t, err)

	agent := NewConsumerWorker(consumer, client)
	defer agent.Close()

	var err0 error
	agent.OnError(func(err error) { err0 = err })

	go agent.Start()
	timekit.Sleep("1ms")

	FatalIfWrongError(t, err0, string(BadKafkaMessageError))
}

func sampleConsumerMessage() *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Value: []byte(`{"hello": "world", "_ctx": {"kafka_topic": "some_topic","kafka_partition": 3,"kafka_replication_factor": 1,"es_index_prefix": "some-type","es_document_type": "some-type"}}`),
	}
}

func sampleMessageChannel(messages ...*sarama.ConsumerMessage) <-chan *sarama.ConsumerMessage {
	messageCh := make(chan *sarama.ConsumerMessage)
	go func() {
		for _, message := range messages {
			messageCh <- message
		}
	}()
	timekit.Sleep("1s")

	return messageCh
}

func sampleNotificationChannel(notifications ...*cluster.Notification) chan *cluster.Notification {
	notificationCh := make(chan *cluster.Notification)
	go func() {
		for _, notification := range notifications {
			notificationCh <- notification
		}
	}()
	timekit.Sleep("1s")

	return notificationCh
}

func sampleErrorChannel(errs ...error) chan error {
	errorCh := make(chan error)
	go func() {
		for _, err := range errs {
			errorCh <- err
		}
	}()

	return errorCh
}
