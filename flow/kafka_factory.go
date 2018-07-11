package flow

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type KafkaFactory interface {
	MakeKafkaAdmin() (admin KafkaAdmin, err error)
	MakeClusterConsumer(groupID, topic string) (worker ClusterConsumer, err error)
}

type kafkaFactory struct {
	brokers []string
	config  *sarama.Config
}

func NewKafkaFactory(brokers []string, config *sarama.Config) KafkaFactory {
	return &kafkaFactory{
		brokers: brokers,
		config:  config,
	}
}

func (f kafkaFactory) MakeKafkaAdmin() (admin KafkaAdmin, err error) {
	admin, err = NewKafkaAdmin(f.brokers, f.config)
	return
}

func (f kafkaFactory) MakeClusterConsumer(groupID, topic string) (consumer ClusterConsumer, err error) {
	clusterConfig := cluster.NewConfig()
	clusterConfig.Config = *f.config
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true

	topics := []string{topic}
	consumer, err = cluster.NewConsumer(f.brokers, groupID, topics, clusterConfig)
	return
}
