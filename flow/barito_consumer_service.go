package flow

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

const (
	BadKafkaMessageError     = errkit.Error("Bad Kafka Message")
	StoreFailedError         = errkit.Error("Store Failed")
	ElasticsearchClientError = errkit.Error("Elasticsearch client error")
	ErrConsumerWorkerFailed  = errkit.Error("Consumer Worker Failed")
)

type BaritoConsumerService interface {
	Start()
	Close()
}

type baritoConsumerService struct {
	factory     KafkaFactory
	groupID     string
	elasticUrl  string
	topicSuffix string

	workers        map[string]ConsumerWorker
	admin          KafkaAdmin
	newTopicWorker ConsumerWorker
	spawnMutex     sync.Mutex
}

func NewBaritoConsumerService(
	factory KafkaFactory,
	groupID, elasticURL, topicSuffix, newTopicEventName string) (BaritoConsumerService, error) {

	admin, err := factory.MakeKafkaAdmin()
	if err != nil {
		return nil, err
	}

	newTopicWorker, err := factory.MakeConsumerWorker(groupID, newTopicEventName)
	if err != nil {
		return nil, err
	}

	return &baritoConsumerService{
		factory:        factory,
		groupID:        groupID,
		elasticUrl:     elasticURL,
		topicSuffix:    topicSuffix,
		workers:        make(map[string]ConsumerWorker),
		admin:          admin,
		newTopicWorker: newTopicWorker,
	}, nil
}

func (s *baritoConsumerService) Start() {

	log.Infof("Start Barito Consumer Service")

	if s.newTopicWorker != nil {
		s.startEventsWorker()
	}

	for _, topic := range s.topicsWithSuffix() {
		s.spawnLogsWorker(topic)
	}
	return
}

// Close
func (s baritoConsumerService) Close() {
	for _, worker := range s.workers {
		worker.Close()
	}

	if s.admin != nil {
		s.admin.Close()
	}

	if s.newTopicWorker != nil {
		s.newTopicWorker.Close()
	}
}

func (s *baritoConsumerService) startEventsWorker() {
	s.newTopicWorker.OnSuccess(s.onNewTopicEvent)
	s.newTopicWorker.OnError(s.onError)
	go s.newTopicWorker.Start()
}

func (s *baritoConsumerService) spawnLogsWorker(topic string) (worker ConsumerWorker) {

	worker, err := s.factory.MakeConsumerWorker(s.groupID, topic)
	if err != nil {
		s.onError(errkit.Concat(ErrConsumerWorkerFailed, err))
		return
	}

	log.Infof("Spawn new worker for topic '%s'", topic)

	worker.OnError(s.onError)
	worker.OnSuccess(s.onStoreTimber)
	go worker.Start()

	s.workers[topic] = worker

	return
}

func (s *baritoConsumerService) onError(err error) {
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

func (s *baritoConsumerService) onStoreTimber(message *sarama.ConsumerMessage) {

	fmt.Println("on store timber")

	// elastic client
	client, err := elasticNewClient(s.elasticUrl)
	if err != nil {
		s.onError(errkit.Concat(ElasticsearchClientError, err))
	}

	timber, err := ConvertKafkaMessageToTimber(message)

	if err != nil {
		s.onError(errkit.Concat(BadKafkaMessageError, err))
	} else {
		ctx := context.Background()
		err = elasticStore(client, ctx, timber)
		if err != nil {
			s.onError(errkit.Concat(StoreFailedError, err))
		}
	}
}

func (s *baritoConsumerService) onNewTopicEvent(message *sarama.ConsumerMessage) {
	topic := string(message.Value)

	s.spawnMutex.Lock()
	s.spawnLogsWorker(topic)
	s.spawnMutex.Unlock()
}
