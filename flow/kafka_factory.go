package flow

import (
	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type kafkaFactory struct {
	brokers []string
	config  *sarama.Config
}

func NewKafkaFactory(brokers []string, config *sarama.Config) types.KafkaFactory {
	return &kafkaFactory{
		brokers: brokers,
		config:  config,
	}
}

func (f kafkaFactory) MakeKafkaAdmin() (admin types.KafkaAdmin, err error) {
	client, err := sarama.NewClient(f.brokers, f.config)
	if err != nil {
		return nil, err
	}

	admin, err = NewKafkaAdmin(client)
	if err != nil {
		return nil, err
	}

	return
}

func (f kafkaFactory) MakeClusterConsumer(groupID, topic string, initialOffset int64) (consumer types.ClusterConsumer, err error) {

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

func (f kafkaFactory) MakeConsumerWorker(name string, consumer types.ClusterConsumer) types.ConsumerWorker {
	return NewConsumerWorker(name, consumer)
}
