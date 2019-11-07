package flow

import (
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type KafkaAdmin interface {
	RefreshTopics() error
	SetTopics([]string)
	Topics() []string
	AddTopic(topic string)
	Exist(topic string) bool
	CreateTopic(topic string, numPartitions int32, replicationFactor int16) error
	Close()
}

type kafkaAdmin struct {
	topics       []string
	brokers      []string
	client       sarama.Client
	refreshMutex sync.Mutex
}

func NewKafkaAdmin(client sarama.Client) (admin KafkaAdmin, err error) {
	var brokers []string
	for _, broker := range client.Brokers() {
		brokers = append(brokers, broker.Addr())
	}

	return &kafkaAdmin{
		client:  client,
		brokers: brokers,
	}, nil
}

func (a *kafkaAdmin) RefreshTopics() (err error) {
	a.refreshMutex.Lock()
	defer a.refreshMutex.Unlock()
	topics, err := a.client.Topics()
	if err != nil {
		log.Warn(err)
		return
	}

	a.topics = topics
	return
}

func (a *kafkaAdmin) SetTopics(topics []string) {
	a.topics = topics
}

func (a *kafkaAdmin) Topics() []string {
	if len(a.topics) <= 0 {
		a.RefreshTopics()
	}

	return a.topics
}

func (a *kafkaAdmin) Exist(topic string) bool {
	for _, topic0 := range a.Topics() {
		if topic0 == topic {
			return true
		}
	}

	// topics only fetch if empty, so there's possiblity its unfresh
	a.RefreshTopics()

	for _, topic0 := range a.Topics() {
		if topic0 == topic {
			return true
		}
	}

	return false
}

func (a *kafkaAdmin) AddTopic(topic string) {
	a.topics = append(a.topics, topic)
}

func (a *kafkaAdmin) Close() {
	a.client.Close()
}

func (a *kafkaAdmin) CreateTopic(topic string, numPartitions int32, replicationFactor int16) (err error) {
	a.client.RefreshMetadata()
	detail := &sarama.TopicDetail{NumPartitions: numPartitions, ReplicationFactor: replicationFactor}
	clusterAdmin, err := sarama.NewClusterAdmin(a.brokers, a.client.Config())

	if err != nil {
		return
	}

	defer clusterAdmin.Close()

	err = clusterAdmin.CreateTopic(topic, detail, false)
	if err != nil {
		return
	}

	return
}
