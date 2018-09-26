package flow

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	uuid "github.com/gofrs/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	ErrConvertKafkaMessage   = errkit.Error("Convert KafkaMessage Failed")
	ErrStore                 = errkit.Error("Store Failed")
	ErrElasticsearchClient   = errkit.Error("Elasticsearch Client Failed")
	ErrConsumerWorker        = errkit.Error("Consumer Worker Failed")
	ErrMakeKafkaAdmin        = errkit.Error("Make kafka admin failed")
	ErrMakeNewTopicWorker    = errkit.Error("Make new topic worker failed")
	ErrSpawnWorkerOnNewTopic = errkit.Error("Spawn worker on new topic failed")
	ErrSpawnWorker           = errkit.Error("Span worker failed")
	ErrHaltWorker            = errkit.Error("Consumer Worker Halted")

	PrefixEventGroupID = "nte"
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

	workerMap           map[string]ConsumerWorker
	admin               KafkaAdmin
	newTopicEventWorker ConsumerWorker
	eventWorkerGroupID  string

	lastError    error
	lastTimber   Timber
	lastNewTopic string
	isHalt       bool
}

func NewBaritoConsumerService(factory KafkaFactory, groupID, elasticURL, topicSuffix, newTopicEventName string) BaritoConsumerService {

	return &baritoConsumerService{
		factory:           factory,
		groupID:           groupID,
		elasticUrl:        elasticURL,
		topicSuffix:       topicSuffix,
		newTopicEventName: newTopicEventName,
		workerMap:         make(map[string]ConsumerWorker),
	}
}

func (s *baritoConsumerService) Start() (err error) {

	admin, err := s.initAdmin()

	if err != nil {
		return errkit.Concat(ErrMakeKafkaAdmin, err)
	}

	uuid, _ := uuid.NewV4()
	s.eventWorkerGroupID = fmt.Sprintf("%s-%s", PrefixEventGroupID, uuid)
	log.Infof("Generate event worker group id: %s", s.eventWorkerGroupID)

	worker, err := s.initNewTopicWorker(s.eventWorkerGroupID)
	if err != nil {
		return errkit.Concat(ErrMakeNewTopicWorker, err)
	}

	worker.Start()

	for _, topic := range admin.Topics() {
		if strings.HasSuffix(topic, s.topicSuffix) {
			err := s.spawnLogsWorker(topic, sarama.OffsetNewest)
			if err != nil {
				s.logError(errkit.Concat(ErrSpawnWorker, err))
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

func (s *baritoConsumerService) initNewTopicWorker(groupID string) (worker ConsumerWorker, err error) { // TODO: return worker
	topic := s.newTopicEventName

	consumer, err := s.factory.MakeClusterConsumer(groupID, topic, sarama.OffsetNewest)
	if err != nil {
		return
	}

	worker = NewConsumerWorker(topic, consumer)
	worker.OnSuccess(s.onNewTopicEvent)
	worker.OnError(s.logError)

	s.newTopicEventWorker = worker
	return
}

// Close
func (s baritoConsumerService) Close() {
	for _, worker := range s.workerMap {
		worker.Stop()
	}

	if s.admin != nil {
		s.admin.Close()
	}

	if s.newTopicEventWorker != nil {
		s.newTopicEventWorker.Stop()
	}
}

func (s *baritoConsumerService) spawnLogsWorker(topic string, initialOffset int64) (err error) {

	consumer, err := s.factory.MakeClusterConsumer(s.groupID, topic, initialOffset)
	if err != nil {
		err = errkit.Concat(ErrConsumerWorker, err)
		return
	}

	worker := NewConsumerWorker(topic, consumer)
	worker.OnError(s.logError)
	worker.OnSuccess(s.onStoreTimber)
	worker.Start()

	s.workerMap[topic] = worker

	return
}

func (s *baritoConsumerService) logError(err error) {
	s.lastError = err
	log.Warn(err.Error())
}

func (s *baritoConsumerService) logTimber(timber Timber) {
	s.lastTimber = timber
	log.Infof("Timber: %v", timber)
}

func (s *baritoConsumerService) logNewTopic(topic string) {
	s.lastNewTopic = topic
	log.Infof("New topic: %s", topic)
}

func (s *baritoConsumerService) onElasticRetry(err error) {
	s.logError(errkit.Concat(ErrElasticsearchClient, err))
	s.HaltAllWorker()
}

func (s *baritoConsumerService) onStoreTimber(message *sarama.ConsumerMessage) {

	// create elastic client
	retrier := s.elasticRetrier()
	elastic, err := NewElastic(retrier, s.elasticUrl)
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
	err = elastic.Store(ctx, timber)
	if err != nil {
		s.logError(errkit.Concat(ErrStore, err))
		return
	}

	if s.isHalt {
		err = s.ResumeWorker()
		if err != nil {
			s.logError(errkit.Concat(ErrConsumerWorker, err))
			return
		}
	}

	s.logTimber(timber)
}

func (s *baritoConsumerService) onNewTopicEvent(message *sarama.ConsumerMessage) {
	topic := string(message.Value)

	_, ok := s.workerMap[topic]
	if ok {
		return
	}

	err := s.spawnLogsWorker(topic, sarama.OffsetOldest)

	if err != nil {
		s.logError(errkit.Concat(ErrSpawnWorkerOnNewTopic, err))
		return
	}

	s.logNewTopic(topic)
}

func (s *baritoConsumerService) WorkerMap() map[string]ConsumerWorker {
	return s.workerMap
}

func (s *baritoConsumerService) NewTopicEventWorker() ConsumerWorker {
	return s.newTopicEventWorker
}

func (s *baritoConsumerService) HaltAllWorker() {
	if !s.isHalt {
		s.isHalt = true
		s.logError(ErrHaltWorker)
		s.Close()
	}
}

func (s *baritoConsumerService) elasticRetrier() *ElasticRetrier {
	return NewElasticRetrier(30*time.Second, s.onElasticRetry)
}

func (s *baritoConsumerService) ResumeWorker() (err error) {
	s.isHalt = false
	err = s.Start()

	return
}
