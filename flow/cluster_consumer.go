package flow

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// Interfacing cluser.Consumer for testing purpose
type ClusterConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Notifications() <-chan *cluster.Notification
	Errors() <-chan error
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	Close() error
}
