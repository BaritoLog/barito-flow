package flow

import "github.com/Shopify/sarama"

type KafkaFactory interface {
	MakeKafkaAdmin() (admin KafkaAdmin, err error)
	MakeConsumerWorker(groupID, topic string) (worker ConsumerWorker, err error)
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

func (f kafkaFactory) MakeConsumerWorker(groupID, topic string) (worker ConsumerWorker, err error) {
	worker, err = NewConsumerWorker(f.brokers, f.config, groupID, topic)
	return
}
