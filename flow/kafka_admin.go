package flow

import (
	"strings"

	"github.com/Shopify/sarama"
)

type KafkaAdmin interface {
	RefreshTopics() error
	Topics() []string
	TopicsWithSuffix(suffix string) []string
	Close()
}

type kafkaAdmin struct {
	topics []string
	client sarama.Client
}

func NewKafkaAdmin(brokers []string) (KafkaAdmin, error) {
	config := sarama.NewConfig()

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	return &kafkaAdmin{
		client: client,
	}, nil
}

func (a *kafkaAdmin) CreateTopicIfNotExist(topic string) {
	// TODO: fetch cache topics list
	// TODO: check if topic exist
	// TODO: refresh cache if topic not exist and check again
	// TODO: create topic
}

func (a *kafkaAdmin) RefreshTopics() (err error) {
	topics, err := a.client.Topics()
	if err != nil {
		return
	}

	a.topics = topics
	return
}

func (a *kafkaAdmin) Topics() []string {
	if len(a.topics) <= 0 {
		a.RefreshTopics()
	}

	return a.topics
}

func (a *kafkaAdmin) TopicsWithSuffix(suffix string) (topics []string) {
	for _, topic := range a.Topics() {
		if strings.HasSuffix(topic, suffix) {
			topics = append(topics, topic)
		}
	}
	return
}

func (a *kafkaAdmin) Close() {
	a.client.Close()
}
