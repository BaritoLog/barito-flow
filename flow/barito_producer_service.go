package flow

import (
	"net/http"

	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
)

type BaritoProducerService interface {
	Start() error
	Close()
	ServeHTTP(rw http.ResponseWriter, req *http.Request)
}

type baritoProducerService struct {
	producer      sarama.SyncProducer
	topicSuffix   string
	newEventTopic string

	admin  KafkaAdmin
	bucket LeakyBucket
	server *http.Server
}

func NewBaritoProducerService(addr string, brokers []string, config *sarama.Config, maxTps int, topicSuffix string, newEventTopic string) (BaritoProducerService, error) {

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	admin, err := NewKafkaAdmin(brokers, config)
	if err != nil {
		return nil, err
	}

	s := &baritoProducerService{
		producer:      producer,
		admin:         admin,
		topicSuffix:   topicSuffix,
		bucket:        NewLeakyBucket(maxTps, timekit.Duration("1s")),
		newEventTopic: newEventTopic,
	}

	s.server = &http.Server{
		Addr:    addr,
		Handler: s,
	}

	return s, nil
}

func (a *baritoProducerService) Start() error {

	a.bucket.StartRefill()

	return a.server.ListenAndServe()
}

func (a *baritoProducerService) Close() {
	if a.server != nil {
		a.server.Close()
	}

	if a.bucket != nil {
		a.bucket.Close()
	}

	if a.admin != nil {
		a.admin.Close()
	}

	if a.producer != nil {
		a.producer.Close()
	}

}

func (s *baritoProducerService) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if !s.bucket.Take() {
		onLimitExceeded(rw)
		return
	}

	timber, err := ConvertRequestToTimber(req)
	if err != nil {
		onBadRequest(rw, err)
		return
	}

	newTopicCreated, err := s.createTopicIfNotExist(timber)
	if err != nil {
		onCreateTopicError(rw, err)
		return
	}

	topic := timber.Context().KafkaTopic

	if newTopicCreated {
		s.sendCreateTopicEvents(topic)
	}

	err = s.sendLogs(topic, timber)
	if err != nil {
		onStoreError(rw, err)
		return
	}

	onSuccess(rw, ProduceResult{
		Topic:      topic,
		IsNewTopic: newTopicCreated,
	})
}

func (s *baritoProducerService) sendLogs(topic string, timber Timber) (err error) {
	message := ConvertTimberToKafkaMessage(timber, topic+s.topicSuffix)
	_, _, err = s.producer.SendMessage(message)
	return
}

func (s *baritoProducerService) sendCreateTopicEvents(topic string) (err error) {
	message := &sarama.ProducerMessage{
		Topic: s.newEventTopic,
		Value: sarama.ByteEncoder(topic),
	}
	_, _, err = s.producer.SendMessage(message)
	return
}

func (s *baritoProducerService) createTopicIfNotExist(timber Timber) (creatingTopic bool, err error) {
	topic := timber.Context().KafkaTopic
	numPartitions := timber.Context().KafkaPartition
	replicationFactor := timber.Context().KafkaReplicationFactor

	if s.admin.Exist(topic) {
		return
	}

	err = s.admin.CreateTopic(topic, numPartitions, replicationFactor)
	if err != nil {
		return
	}

	s.admin.AddTopic(topic)
	creatingTopic = true
	return
}
