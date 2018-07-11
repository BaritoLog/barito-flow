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
	BadKafkaMessageError        = errkit.Error("Bad Kafka Message")
	StoreFailedError            = errkit.Error("Store Failed")
	ElasticsearchClientError    = errkit.Error("Elasticsearch client error")
	ErrConsumerWorkerFailed     = errkit.Error("Consumer Worker Failed")
	ErrMakeKafkaAdminFailed     = errkit.Error("Make kafka admin failed")
	ErrMakeNewTopicWorkerFailed = errkit.Error("Make new topic worker failed")
)

type BaritoConsumerService interface {
	Start() error
	Close()
	WorkerMap() map[string]ConsumerWorker
	NewTopicEventWorker() ConsumerWorker
}

type baritoConsumerService struct {
	factory           KafkaFactory
	groupID           string
	elasticUrl        string
	topicSuffix       string
	newTopicEventName string

	workers             map[string]ConsumerWorker
	admin               KafkaAdmin
	newTopicEventWorker ConsumerWorker
	spawnMutex          sync.Mutex
}

func NewBaritoConsumerService(factory KafkaFactory, groupID, elasticURL, topicSuffix, newTopicEventName string) BaritoConsumerService {

	return &baritoConsumerService{
		factory:           factory,
		groupID:           groupID,
		elasticUrl:        elasticURL,
		topicSuffix:       topicSuffix,
		newTopicEventName: newTopicEventName,
		workers:           make(map[string]ConsumerWorker),
	}
}

func (s *baritoConsumerService) Start() (err error) {

	log.Infof("Start Barito Consumer Service")

	admin, err := s.initAdmin()
	if err != nil {
		return errkit.Concat(ErrMakeKafkaAdminFailed, err)
	}

	worker, err := s.initNewTopicWorker()
	if err != nil {
		return errkit.Concat(ErrMakeNewTopicWorkerFailed, err)
	}

	worker.Start()

	for _, topic := range admin.Topics() {
		if strings.HasSuffix(topic, s.topicSuffix) {
			err := s.spawnLogsWorker(topic)
			if err != nil {
				s.onError(err)
			}
		}
	}

	return
}

func (s *baritoConsumerService) initAdmin() (admin KafkaAdmin, err error) {
	admin, err = s.factory.MakeKafkaAdmin()
	s.admin = admin
	return
}

func (s *baritoConsumerService) initNewTopicWorker() (worker ConsumerWorker, err error) { // TODO: return worker
	consumer, err := s.factory.MakeClusterConsumer(s.groupID, s.newTopicEventName)
	if err != nil {
		return
	}

	worker = NewConsumerWorker(consumer)
	worker.OnSuccess(s.onNewTopicEvent)
	worker.OnError(s.onError)

	s.newTopicEventWorker = worker
	return
}

// Close
func (s baritoConsumerService) Close() {
	for _, worker := range s.workers {
		worker.Stop()
	}

	if s.admin != nil {
		s.admin.Close()
	}

	if s.newTopicEventWorker != nil {
		s.newTopicEventWorker.Stop()
	}
}

func (s *baritoConsumerService) spawnLogsWorker(topic string) (err error) {

	consumer, err := s.factory.MakeClusterConsumer(s.groupID, topic)
	if err != nil {
		err = errkit.Concat(ErrConsumerWorkerFailed, err)
		return
	}

	log.Infof("Spawn new worker for topic '%s'", topic)

	worker := NewConsumerWorker(consumer)
	worker.OnError(s.onError)
	worker.OnSuccess(s.onStoreTimber)
	worker.Start()

	s.workers[topic] = worker

	return
}

func (s *baritoConsumerService) onError(err error) {
	log.Warn(err.Error())
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
	err := s.spawnLogsWorker(topic)
	if err != nil {
		s.onError(err)
	}
	s.spawnMutex.Unlock()
}

func (s *baritoConsumerService) WorkerMap() map[string]ConsumerWorker {
	return s.workers
}

func (s *baritoConsumerService) NewTopicEventWorker() ConsumerWorker {
	return s.newTopicEventWorker

}
