package flow

import (
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
)

type BaritoConsumerService interface {
	Start() error
	Close()
}

type baritoConsumerService struct {
	brokers     []string
	groupID     string
	elasticUrl  string
	topicSuffix string
	workers     map[string]ConsumerWorker
	admin       KafkaAdmin

	config *sarama.Config
}

func NewBaritoConsumerService(brokers []string, config *sarama.Config, groupID, elasticURL, topicSuffix string) (BaritoConsumerService, error) {

	admin, err := NewKafkaAdmin(brokers, config)
	if err != nil {
		return nil, err
	}

	return &baritoConsumerService{
		brokers:     brokers,
		groupID:     groupID,
		elasticUrl:  elasticURL,
		topicSuffix: topicSuffix,
		workers:     make(map[string]ConsumerWorker),
		config:      config,
		admin:       admin,
	}, nil
}

func (s baritoConsumerService) Start() (err error) {

	log.Infof("Start Barito Consumer Service")

	topics := s.topicsWithSuffix()

	for _, topic := range topics {
		log.Infof("Spawn new worker for topic '%s'", topic)
		var worker ConsumerWorker
		worker, err = s.spawnNewWorker(topic)
		if err != nil {
			return
		}

		// TODO: worker's instrumentation

		s.workers[topic] = worker
		go worker.Start()
	}
	return
}

func (s baritoConsumerService) Close() {
	for _, worker := range s.workers {
		worker.Close()
	}

	if s.admin != nil {
		s.admin.Close()
	}

}

func (s baritoConsumerService) spawnNewWorker(topic string) (worker ConsumerWorker, err error) {

	// elastic client
	client, err := elasticNewClient(s.elasticUrl)

	// consumer config
	config := cluster.NewConfig()
	config.Config = *s.config
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// kafka consumer
	consumer, err := cluster.NewConsumer(s.brokers, s.groupID,
		[]string{topic}, config)
	if err != nil {
		return
	}

	worker = NewConsumerWorker(consumer, client)
	worker.OnError(s.onErrror)
	return
}

func (s baritoConsumerService) onErrror(err error) {
	log.Warn(err.Error())
}

func (s *baritoConsumerService) topicsWithSuffix() (topics []string) {
	for _, topic := range s.admin.Topics() {
		if strings.HasSuffix(topic, s.topicSuffix) {
			topics = append(topics, topic)
		}
	}
	return
}
