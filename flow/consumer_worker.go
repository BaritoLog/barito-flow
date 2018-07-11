package flow

import (
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

const (
	RetrieveMessageFailedError = errkit.Error("Retrieve message failed")
)

type ConsumerWorker interface {
	Start()
	Stop()
	IsStart() bool
	OnError(f func(error))
	OnSuccess(f func(*sarama.ConsumerMessage))
	OnNotification(f func(*cluster.Notification))
}

type consumerWorker struct {
	isStart            bool
	consumer           ClusterConsumer
	onErrorFunc        func(error)
	onSuccessFunc      func(*sarama.ConsumerMessage)
	onNotificationFunc func(*cluster.Notification)
	stop               chan int
}

func NewConsumerWorker(consumer ClusterConsumer) ConsumerWorker {
	return &consumerWorker{
		consumer: consumer,
		stop:     make(chan int),
	}
}

func (w *consumerWorker) Start() {
	go w.loopErrors()
	go w.loopNotification()
	go w.loopMain()
	w.isStart = true
}

func (w *consumerWorker) Stop() {
	if w.consumer != nil {
		w.consumer.Close()
	}

	go func() {
		w.stop <- 1
	}()

	w.isStart = false
}

func (w *consumerWorker) IsStart() bool {
	return w.isStart
}

func (w *consumerWorker) OnError(f func(error)) {
	w.onErrorFunc = f
}

func (w *consumerWorker) OnSuccess(f func(*sarama.ConsumerMessage)) {
	w.onSuccessFunc = f
}

func (w *consumerWorker) OnNotification(f func(*cluster.Notification)) {
	w.onNotificationFunc = f
}

func (w *consumerWorker) loopMain() {
	for {
		select {
		case message, ok := <-w.consumer.Messages():
			if ok {
				w.fireSuccess(message)
				w.consumer.MarkOffset(message, "")
			}
		case <-w.stop:
			return
		}
	}
}

func (w *consumerWorker) loopNotification() {
	for notification := range w.consumer.Notifications() {
		w.fireNotification(notification)
	}
}

func (w *consumerWorker) loopErrors() {
	for err := range w.consumer.Errors() {
		w.fireError(errkit.Concat(RetrieveMessageFailedError, err))
	}
}

func (w *consumerWorker) fireSuccess(message *sarama.ConsumerMessage) {
	if w.onSuccessFunc != nil {
		w.onSuccessFunc(message)
	}
}

func (a *consumerWorker) fireError(err error) {
	if a.onErrorFunc != nil {
		a.onErrorFunc(err)
	}
}

func (a *consumerWorker) fireNotification(notification *cluster.Notification) {
	if a.onNotificationFunc != nil {
		a.onNotificationFunc(notification)
	}
}
