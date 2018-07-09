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

	want := &sarama.ConsumerMessage{}

	consumer := mock.NewMockClusterConsumer(ctrl)
	consumer.EXPECT().Messages().AnyTimes().
		Return(sampleMessageChannel(want))
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

	var got *sarama.ConsumerMessage
	var gotNotification *cluster.Notification

	agent := &consumerWorker{Consumer: consumer}
	agent.OnSuccess(func(message *sarama.ConsumerMessage) { got = message })
	agent.OnNotification(func(notification *cluster.Notification) { gotNotification = notification })
	defer agent.Close()

	go agent.Start()
	timekit.Sleep("2ms")

	FatalIf(t, got != want, "wrong message")
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

	var err0 error
	agent := &consumerWorker{Consumer: consumer}
	agent.OnError(func(err error) { err0 = err })
	defer agent.Close()

	go agent.Start()
	timekit.Sleep("1ms")

	FatalIfWrongError(t, err0, "expected kafka error")
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
