package flow

import (
	"context"
	"strings"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

const (
	BadKafkaMessageError     = errkit.Error("Bad Kafka Message")
	StoreFailedError         = errkit.Error("Store Failed")
	ElasticsearchClientError = errkit.Error("Elasticsearch client error")
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

func NewBaritoConsumerService(
	brokers []string,
	config *sarama.Config,
	groupID, elasticURL, topicSuffix, newTopicEventName string) (BaritoConsumerService, error) {

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
	groupID := "barito" // TODO: as parametr

	worker, err = NewConsumerWorker(s.brokers, s.config, groupID, topic)
	if err != nil {
		return
	}

	worker.OnError(s.onErrror)
	worker.OnSuccess(s.storeTimber)
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

func (s *baritoConsumerService) storeTimber(message *sarama.ConsumerMessage) {
	// elastic client
	client, err := elasticNewClient(s.elasticUrl)
	if err != nil {
		s.onErrror(errkit.Concat(ElasticsearchClientError, err))
	}

	timber, err := ConvertKafkaMessageToTimber(message)

	if err != nil {
		s.onErrror(errkit.Concat(BadKafkaMessageError, err))
	} else {
		ctx := context.Background()
		err = elasticStore(client, ctx, timber)
		if err != nil {
			s.onErrror(errkit.Concat(StoreFailedError, err))
		}
	}
}
