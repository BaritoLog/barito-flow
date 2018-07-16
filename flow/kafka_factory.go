package flow

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type KafkaFactory interface {
	MakeKafkaAdmin() (admin KafkaAdmin, err error)
	MakeClusterConsumer(groupID, topic string, initialOffset int64) (worker ClusterConsumer, err error)
	MakeSyncProducer() (producer sarama.SyncProducer, err error)
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
	client, err := sarama.NewClient(f.brokers, f.config)
	if err != nil {
		return nil, err
	}

	admin = NewKafkaAdmin(client)
	return
}

func (f kafkaFactory) MakeClusterConsumer(groupID, topic string, initialOffset int64) (consumer ClusterConsumer, err error) {

	config := *f.config
	config.Consumer.Offsets.Initial = initialOffset

	clusterConfig := cluster.NewConfig()
	clusterConfig.Config = config
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true

	topics := []string{topic}
	consumer, err = cluster.NewConsumer(f.brokers, groupID, topics, clusterConfig)
	return
}

func (f kafkaFactory) MakeSyncProducer() (producer sarama.SyncProducer, err error) {
	producer, err = sarama.NewSyncProducer(f.brokers, f.config)
	return
}
