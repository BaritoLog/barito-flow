package flow

import (
	cluster "github.com/bsm/sarama-cluster"
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
				timber := NewTimberFromKafkaMessage(message)
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

func (a *KafkaAgent) fireSuccess(timber Timber) {
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
