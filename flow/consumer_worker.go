package flow

import (
	"github.com/BaritoLog/barito-flow/prome"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
)

const (
	RetrieveMessageFailedError  = errkit.Error("Retrieve message failed")
)

type ConsumerWorker interface {
	Start()
	Stop()
	Halt()
	IsStart() bool
	OnError(f func(error))
	OnSuccess(f func(*sarama.ConsumerMessage))
	OnNotification(f func(*cluster.Notification))
}

type consumerWorker struct {
	name               string
	isStart            bool
	consumer           ClusterConsumer
	onErrorFunc        func(error)
	onSuccessFunc      func(*sarama.ConsumerMessage)
	onNotificationFunc func(*cluster.Notification)
	stop               chan int
	lastMessage        *sarama.ConsumerMessage
}

func NewConsumerWorker(name string, consumer ClusterConsumer) ConsumerWorker {
	return &consumerWorker{
		name:     name,
		consumer: consumer,
		stop:     make(chan int),
	}
}

func (w *consumerWorker) Start() {
	log.Warnf("Start worker '%s'", w.name)

	go w.loopErrors()
	go w.loopNotification()
	go w.loopMain()
}

func (w *consumerWorker) Stop() {
	if w.consumer != nil {
		w.consumer.Close()
	}

	go func() {
		w.stop <- 1
	}()
}

func (w *consumerWorker) Halt() {
	go func() {
		w.stop <- 1
	}()
	log.Warnf("Halt worker '%s'", w.name)
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
	w.isStart = true
	for {
		select {
		case message, ok := <-w.consumer.Messages():
			if ok {
				prome.IncreaseKafkaMessagesIncoming(message.Topic)
				w.consumer.MarkOffset(message, "")
				w.fireSuccess(message)
				log.Infof("Mark Offset, %v", message)
			} else {
				prome.IncreaseConsumerTimberConvertError(TimberConvertErrorIndexName + "_" + "loopMain")
				log.Warnf("failed to receive incoming message")
				w.Stop()
			}
		case <-w.stop:
			w.isStart = false
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

func (w *consumerWorker) fireError(err error) {
	if w.onErrorFunc != nil {
		w.onErrorFunc(err)
	}
}

func (w *consumerWorker) fireNotification(notification *cluster.Notification) {
	if w.onNotificationFunc != nil {
		w.onNotificationFunc(notification)
	}
}
