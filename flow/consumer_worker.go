package flow

import (
	"context"

	"github.com/BaritoLog/go-boilerplate/errkit"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/olivere/elastic"
)

const (
	BadKafkaMessageError = errkit.Error("Bad Kafka Message")
	StoreFailedError     = errkit.Error("Store Failed")
	GeneralKafkaError    = errkit.Error("Got Kafka error")
)

type ConsumerWorker interface {
	Start() error
	Close()
	OnError(f func(error))
	OnSuccess(f func(Timber))
	OnNotification(f func(*cluster.Notification))
}

type consumerWorker struct {
	Consumer           KafkaConsumer
	Client             *elastic.Client
	onErrorFunc        func(error)
	onSuccessFunc      func(Timber)
	onNotificationFunc func(*cluster.Notification)
}

func NewConsumerWorker(consumer KafkaConsumer, client *elastic.Client) ConsumerWorker {
	return &consumerWorker{
		Consumer: consumer,
		Client:   client,
	}
}

func (w *consumerWorker) OnError(f func(error)) {
	w.onErrorFunc = f
}

func (w *consumerWorker) OnSuccess(f func(Timber)) {
	w.onSuccessFunc = f
}

func (w *consumerWorker) OnNotification(f func(*cluster.Notification)) {
	w.onNotificationFunc = f
}

func (a *consumerWorker) Start() (err error) {
	go a.loopErrors()
	go a.loopNotification()

	a.loopMain()
	return
}

func (a *consumerWorker) Close() {
	if a.Consumer != nil {
		a.Consumer.Close()
	}
}

func (a *consumerWorker) loopMain() {
	for {
		select {
		case message, ok := <-a.Consumer.Messages():
			if ok {
				timber, err := ConvertKafkaMessageToTimber(message)

				if err != nil {
					a.fireError(BadKafkaMessageError, err)
				} else {
					ctx := context.Background()
					err = elasticStore(a.Client, ctx, timber)
					if err != nil {
						a.fireError(StoreFailedError, err)
					} else {
						a.fireSuccess(timber)
					}
				}

				a.Consumer.MarkOffset(message, "")

			}
		}
	}
}

func (a *consumerWorker) loopNotification() {
	for notification := range a.Consumer.Notifications() {
		a.fireNotification(notification)
	}
}

func (a *consumerWorker) loopErrors() {
	for err := range a.Consumer.Errors() {
		a.fireError(GeneralKafkaError, err)
	}
}

func (a *consumerWorker) fireSuccess(timber Timber) {
	if a.onSuccessFunc != nil {
		a.onSuccessFunc(timber)
	}
}

func (a *consumerWorker) fireError(prefix, err error) {
	if a.onErrorFunc != nil {
		a.onErrorFunc(errkit.Concat(prefix, err))
	}
}

func (a *consumerWorker) fireNotification(notification *cluster.Notification) {
	if a.onNotificationFunc != nil {
		a.onNotificationFunc(notification)
	}
}
