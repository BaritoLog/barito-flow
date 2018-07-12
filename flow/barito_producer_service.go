package flow

import (
	"net/http"

	"github.com/Shopify/sarama"
)

type BaritoProducerService interface {
	Start() error
	Close()
	ServeHTTP(rw http.ResponseWriter, req *http.Request)
}

type baritoProducerService struct {
	factory       KafkaFactory
	addr          string
	topicSuffix   string
	newEventTopic string

	producer sarama.SyncProducer
	admin    KafkaAdmin
	server   *http.Server
}

func NewBaritoProducerService(factory KafkaFactory, addr string, maxTps int, topicSuffix string, newEventTopic string) BaritoProducerService {

	return &baritoProducerService{
		factory:       factory,
		addr:          addr,
		topicSuffix:   topicSuffix,
		newEventTopic: newEventTopic,
	}

}

func (s *baritoProducerService) Start() (err error) {

	// TODO: change to ratelimiter
	// a.bucket.StartRefill()

	s.producer, err = s.factory.MakeSyncProducer()
	if err != nil {
		return
	}

	s.admin, err = s.factory.MakeKafkaAdmin()
	if err != nil {
		return
	}

	server := s.initHttpServer()

	return server.ListenAndServe()
}

func (s *baritoProducerService) initHttpServer() (server *http.Server) {
	server = &http.Server{
		Addr:    s.addr,
		Handler: s,
	}

	s.server = server

	return

}

func (a *baritoProducerService) Close() {
	if a.server != nil {
		a.server.Close()
	}

	// TODO: change to rate limiter
	// if a.bucket != nil {
	// 	a.bucket.Close()
	// }

	if a.admin != nil {
		a.admin.Close()
	}

	if a.producer != nil {
		a.producer.Close()
	}

}

func (s *baritoProducerService) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	// TODO: change to reate limit
	// if !s.bucket.Take() {
	// 	onLimitExceeded(rw)
	// 	return
	// }

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
