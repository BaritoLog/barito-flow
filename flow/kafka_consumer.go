package flow

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// Interfacing cluser.Consumer for testing purpose
type KafkaConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Notifications() <-chan *cluster.Notification
	Errors() <-chan error
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	Close() error
}

type dummyKafkaConsumer struct {
	messages      chan *sarama.ConsumerMessage
	notifications chan *cluster.Notification
	errors        chan error
}

func (c *dummyKafkaConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

func (c *dummyKafkaConsumer) Errors() <-chan error {
	return c.errors
}

func (c *dummyKafkaConsumer) Notifications() <-chan *cluster.Notification {
	return c.notifications
}

func (c *dummyKafkaConsumer) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
}

func (c *dummyKafkaConsumer) Close() error {
	return nil
}
