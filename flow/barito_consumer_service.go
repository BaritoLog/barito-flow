package flow

import (
	"context"
	"strings"
	"sync"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

const (
	ErrConvertKafkaMessage = errkit.Error("Convert KafkaMessage Failed")
	ErrStore               = errkit.Error("Store Failed")
	ErrElasticsearchClient = errkit.Error("Elasticsearch Client Failed")
	ErrConsumerWorker      = errkit.Error("Consumer Worker Failed")
	ErrMakeKafkaAdmin      = errkit.Error("Make kafka admin failed")
	ErrMakeNewTopicWorker  = errkit.Error("Make new topic worker failed")
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

	lastError  error
	lastTimber Timber
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
		return errkit.Concat(ErrMakeKafkaAdmin, err)
	}

	worker, err := s.initNewTopicWorker()
	if err != nil {
		return errkit.Concat(ErrMakeNewTopicWorker, err)
	}

	worker.Start()

	for _, topic := range admin.Topics() {
		if strings.HasSuffix(topic, s.topicSuffix) {
			err := s.spawnLogsWorker(topic)
			if err != nil {
				s.logError(err)
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
	worker.OnError(s.logError)

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
		err = errkit.Concat(ErrConsumerWorker, err)
		return
	}

	log.Infof("Spawn new worker for topic '%s'", topic)

	worker := NewConsumerWorker(consumer)
	worker.OnError(s.logError)
	worker.OnSuccess(s.onStoreTimber)
	worker.Start()

	s.workers[topic] = worker

	return
}

func (s *baritoConsumerService) logError(err error) {
	s.lastError = err
	log.Warn(err.Error())
}

func (s *baritoConsumerService) logTimber(timber Timber) {
	s.lastTimber = timber
	log.Info(timber)
}

func (s *baritoConsumerService) onStoreTimber(message *sarama.ConsumerMessage) {

	// create elastic client
	client, err := elasticNewClient(s.elasticUrl)
	if err != nil {
		s.logError(errkit.Concat(ErrElasticsearchClient, err))
		return
	}

	// convert kafka message
	timber, err := ConvertKafkaMessageToTimber(message)
	if err != nil {
		s.logError(errkit.Concat(ErrConvertKafkaMessage, err))
		return
	}

	// store to elasticsearch
	ctx := context.Background()
	err = elasticStore(client, ctx, timber)
	if err != nil {
		s.logError(errkit.Concat(ErrStore, err))
		return
	}

	s.logTimber(timber)
}

func (s *baritoConsumerService) onNewTopicEvent(message *sarama.ConsumerMessage) {
	topic := string(message.Value)

	s.spawnMutex.Lock()
	err := s.spawnLogsWorker(topic)
	if err != nil {
		s.logError(err)
	}
	s.spawnMutex.Unlock()
}

func (s *baritoConsumerService) WorkerMap() map[string]ConsumerWorker {
	return s.workers
}

func (s *baritoConsumerService) NewTopicEventWorker() ConsumerWorker {
	return s.newTopicEventWorker
}
