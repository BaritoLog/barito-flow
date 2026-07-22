package flow

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/mock"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/IBM/sarama"
	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestConsumerWorker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	want := &sarama.ConsumerMessage{Topic: "test"}
	wantNotification := &types.Notification{}

	consumer := mock.NewMockClusterConsumer(ctrl)
	consumer.EXPECT().Messages().AnyTimes().Return(sampleMessageChannel(want))
	consumer.EXPECT().Notifications().Return(sampleNotificationChannel(wantNotification))
	consumer.EXPECT().Errors().Return(sampleErrorChannel())
	consumer.EXPECT().MarkOffset(gomock.Any(), gomock.Any())
	consumer.EXPECT().Close()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	var got *sarama.ConsumerMessage
	var gotNotification *types.Notification

	worker := NewConsumerWorker("worker", consumer, DefaultNumProcessWorkers)
	worker.OnSuccess(func(message *sarama.ConsumerMessage) { got = message })
	worker.OnNotification(func(notification *types.Notification) { gotNotification = notification })

	worker.Start()
	defer worker.Stop()

	time.Sleep(2 * time.Millisecond)

	FatalIf(t, got != want, "wrong message")
	FatalIf(t, gotNotification != wantNotification, "wrong notification")

	expected := `
		# HELP barito_consumer_kafka_message_incoming_total Number of messages incoming from kafka
		# TYPE barito_consumer_kafka_message_incoming_total counter
		barito_consumer_kafka_message_incoming_total{topic="test"} 1
	`
	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_consumer_kafka_message_incoming_total"))
}

func TestConsumerWorker_KafkaError(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock.NewMockClusterConsumer(ctrl)
	consumer.EXPECT().Messages().AnyTimes().Return(sampleMessageChannel())
	consumer.EXPECT().Notifications().Return(sampleNotificationChannel())
	consumer.EXPECT().Errors().Return(sampleErrorChannel(fmt.Errorf("expected kafka error")))
	consumer.EXPECT().Close()

	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusOK,
	})
	defer ts.Close()

	var gotErr error

	worker := NewConsumerWorker("worker", consumer, DefaultNumProcessWorkers)
	worker.OnError(func(err error) { gotErr = err })

	worker.Start()
	defer worker.Stop()

	time.Sleep(1 * time.Millisecond)

	FatalIfWrongError(t, gotErr, "expected kafka error")
}

func sampleMessageChannel(messages ...*sarama.ConsumerMessage) <-chan *sarama.ConsumerMessage {
	messageCh := make(chan *sarama.ConsumerMessage)
	go func() {
		for _, message := range messages {
			messageCh <- message
		}
	}()
	time.Sleep(1 * time.Second)

	return messageCh
}

func sampleNotificationChannel(notifications ...*types.Notification) chan *types.Notification {
	notificationCh := make(chan *types.Notification)
	go func() {
		for _, notification := range notifications {
			notificationCh <- notification
		}
	}()
	time.Sleep(1 * time.Second)

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
