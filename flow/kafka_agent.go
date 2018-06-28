package flow

import (
	"github.com/BaritoLog/go-boilerplate/errkit"
	cluster "github.com/bsm/sarama-cluster"
)

const (
	BadKafkaMessageError = errkit.Error("Bad Kafka Message")
	StoreFailedError     = errkit.Error("Store Failed")
	GeneralKafkaError    = errkit.Error("Got Kafka error")
)

type KafkaAgent struct {
	Consumer       KafkaConsumer
	Store          func(Timber) error
	OnError        func(error)
	OnSuccess      func(Timber)
	OnNotification func(*cluster.Notification)
}

func (a *KafkaAgent) Start() (err error) {
	go a.loopErrors()
	go a.loopNotification()

	a.loopMain()
	return
}

func (a *KafkaAgent) Close() {
	if a.Consumer != nil {
		a.Consumer.Close()
	}
}

func (a *KafkaAgent) loopMain() {
	for {
		select {
		case message, ok := <-a.Consumer.Messages():
			if ok {
				timber, err := NewTimberFromKafkaMessage(message)

				if err != nil {
					a.fireError(BadKafkaMessageError, err)
				} else {
					err = a.Store(timber)
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

func (a *KafkaAgent) loopNotification() {
	for notification := range a.Consumer.Notifications() {
		a.fireNotification(notification)
	}
}

func (a *KafkaAgent) loopErrors() {
	for err := range a.Consumer.Errors() {
		a.fireError(GeneralKafkaError, err)
	}
}

func (a *KafkaAgent) fireSuccess(timber Timber) {
	if a.OnSuccess != nil {
		a.OnSuccess(timber)
	}
}

func (a *KafkaAgent) fireError(prefix, err error) {
	if a.OnError != nil {
		a.OnError(errkit.Concat(prefix, err))
	}
}

func (a *KafkaAgent) fireNotification(notification *cluster.Notification) {
	if a.OnNotification != nil {
		a.OnNotification(notification)
	}
}
