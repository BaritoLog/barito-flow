package flow

import (
	"fmt"
	"testing"

	"github.com/BaritoLog/barito-flow/river"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func TestKafkaAgent(t *testing.T) {
	messages := make(chan *sarama.ConsumerMessage)
	notifications := make(chan *cluster.Notification)

	expectedNotification := &cluster.Notification{}

	go func() {
		messages <- &sarama.ConsumerMessage{
			Value: []byte("expected message"),
		}
		notifications <- expectedNotification
	}()
	timekit.Sleep("1ms")

	var gotTimber river.Timber
	var gotNotification *cluster.Notification

	agent := KafkaAgent{
		Consumer: &dummyKafkaConsumer{
			messages:      messages,
			notifications: notifications,
			errors:        make(chan error),
		},
		Store: func(timber river.Timber) error {
			return nil
		},
		OnSuccess: func(timber river.Timber) {
			gotTimber = timber
		},
		OnNotification: func(notification *cluster.Notification) {
			gotNotification = notification
		},
	}

	go agent.Start()
	timekit.Sleep("1ms")

	FatalIf(t, gotTimber.Message() != "expected message", "wrong timber message")
}

func TestKafkaAgent_StoreError(t *testing.T) {
	messages := make(chan *sarama.ConsumerMessage)
	go func() {
		messages <- &sarama.ConsumerMessage{
			Value: []byte("some message"),
		}
	}()
	timekit.Sleep("1ms")

	var err0 error

	agent := KafkaAgent{
		Consumer: &dummyKafkaConsumer{
			messages:      messages,
			notifications: make(chan *cluster.Notification),
			errors:        make(chan error),
		},
		Store: func(timber river.Timber) error {
			return fmt.Errorf("expected store error")
		},
		OnError: func(err error) {
			err0 = err
		},
	}

	go agent.Start()
	timekit.Sleep("1ms")

	FatalIfWrongError(t, err0, "expected store error")
}

func TestKafkaAgent_KafkaError(t *testing.T) {
	errors := make(chan error)
	go func() {
		errors <- fmt.Errorf("expected kafka error")
	}()
	timekit.Sleep("1ms")

	var err0 error

	agent := KafkaAgent{
		Consumer: &dummyKafkaConsumer{
			messages:      make(chan *sarama.ConsumerMessage),
			notifications: make(chan *cluster.Notification),
			errors:        errors,
		},
		Store: func(timber river.Timber) error {
			return nil
		},
		OnError: func(err error) {
			err0 = err
		},
	}

	go agent.Start()
	timekit.Sleep("1ms")

	FatalIfWrongError(t, err0, "expected kafka error")
}
