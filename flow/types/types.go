package types

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type KafkaFactory interface {
	MakeKafkaAdmin() (admin KafkaAdmin, err error)
	MakeClusterConsumer(groupID, topic string, initialOffset int64) (worker ClusterConsumer, err error)
	MakeSyncProducer() (producer sarama.SyncProducer, err error)
	MakeConsumerWorker(name string, consumer ClusterConsumer) ConsumerWorker
}

type KafkaAdmin interface {
	RefreshTopics() error
	SetTopics([]string)
	Topics() []string
	AddTopic(topic string)
	Exist(topic string) bool
	CreateTopic(topic string, numPartitions int32, replicationFactor int16) error
	Close()
}

type ClusterConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Notifications() <-chan *cluster.Notification
	Errors() <-chan error
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	CommitOffsets() error
	Close() error
}

type ConsumerWorker interface {
	Start()
	Stop()
	Halt()
	IsStart() bool
	OnError(f func(error))
	OnConsumerFlush() error
	OnSuccess(f func(*sarama.ConsumerMessage))
	OnNotification(f func(*cluster.Notification))
}

type ConsumerOutputFactory interface {
	MakeConsumerOutputGCS(string) (ConsumerOutput, error)
}

type ConsumerOutput interface {
	Start() error
	Stop()
	OnMessage([]byte) error
	AddOnFlushFunc(func() error)
}
