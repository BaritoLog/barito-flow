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
	Close()
	OnError(f func(error))
	OnSuccess(f func(*sarama.ConsumerMessage))
	OnNotification(f func(*cluster.Notification))
}

type consumerWorker struct {
	Consumer           ClusterConsumer
	onErrorFunc        func(error)
	onSuccessFunc      func(*sarama.ConsumerMessage)
	onNotificationFunc func(*cluster.Notification)
}

func NewConsumerWorker(brokers []string, config *sarama.Config, groupID, topic string) (ConsumerWorker, error) {
	// consumer config
	clusterConfig := cluster.NewConfig()
	clusterConfig.Config = *config
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true

	// kafka consumer
	consumer, err := cluster.NewConsumer(brokers, groupID,
		[]string{topic}, clusterConfig)
	if err != nil {
		return nil, err
	}

	return &consumerWorker{
		Consumer: consumer,
	}, nil
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

func (a *consumerWorker) Start() {
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
				a.fireSuccess(message)
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
		a.fireError(errkit.Concat(RetrieveMessageFailedError, err))
	}
}

func (a *consumerWorker) fireSuccess(message *sarama.ConsumerMessage) {
	if a.onSuccessFunc != nil {
		a.onSuccessFunc(message)
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
