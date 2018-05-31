package flow

import (
	"github.com/BaritoLog/barito-flow/river"
	cluster "github.com/bsm/sarama-cluster"
)

type KafkaAgent struct {
	Consumer       KafkaConsumer
	Store          func(river.Timber) error
	OnError        func(error)
	OnSuccess      func(river.Timber)
	OnNotification func(*cluster.Notification)
}

func (a *KafkaAgent) Start() (err error) {
	go a.loopErrors()
	go a.loopNotification()

	a.loopMain()
	return
}

func (a *KafkaAgent) loopMain() {
	for {
		select {
		case message, ok := <-a.Consumer.Messages():
			if ok {
				timber := river.NewTimberFromKafkaMessage(message)
				err := a.Store(timber)
				if err != nil {
					a.fireError(err)
				} else {
					a.fireSuccess(timber)
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
		a.fireError(err)
	}
}

func (a *KafkaAgent) fireSuccess(timber river.Timber) {
	if a.OnSuccess != nil {
		a.OnSuccess(timber)
	}
}

func (a *KafkaAgent) fireError(err error) {
	if a.OnError != nil {
		a.OnError(err)
	}
}

func (a *KafkaAgent) fireNotification(notification *cluster.Notification) {
	if a.OnNotification != nil {
		a.OnNotification(notification)
	}
}
