package flow

import (
	"net/http"
	"time"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

const (
	ErrMakeSyncProducer = errkit.Error("Make sync producer failed")
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
	limiter  RateLimiter
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

	s.producer, err = s.factory.MakeSyncProducer()
	if err != nil {
		err = errkit.Concat(ErrMakeSyncProducer, err)
		return
	}

	s.admin, err = s.factory.MakeKafkaAdmin()
	if err != nil {
		err = errkit.Concat(ErrMakeKafkaAdmin, err)
		return
	}

	s.limiter = NewRateLimiter(timekit.Duration("1s"))
	s.limiter.Start()

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

	if a.limiter != nil {
		a.limiter.Stop()
	}

	if a.admin != nil {
		a.admin.Close()
	}

	if a.producer != nil {
		a.producer.Close()
	}

}

func (s *baritoProducerService) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	var success bool
	var topic string
	if req.URL.Path == "/produce_batch" {
		timberCollection, err := ConvertBatchRequestToTimberCollection(req)

		if err != nil {
			onBadRequest(rw, err)
			return
		}

		for _, timber := range timberCollection.Items {
			timber.SetContext(&timberCollection.Context)
			if timber.Timestamp() == "" {
				timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
			}

			success, topic = s.handleProduce(rw, timber)

			if success == false {
				return
			}
		}
	} else {
		timber, err := ConvertRequestToTimber(req)

		if err != nil {
			onBadRequest(rw, err)
			return
		}

		success, topic = s.handleProduce(rw, timber)
	}

	if success == true {
		onSuccess(rw, ProduceResult{
			Topic: topic,
		})
	}
}

func (s *baritoProducerService) sendLogs(topic string, timber Timber) (err error) {
	message := ConvertTimberToKafkaMessage(timber, topic)
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

func (s *baritoProducerService) handleProduce(rw http.ResponseWriter, timber Timber) (bool, string) {
	var err error

	// add suffix
	topic := timber.Context().KafkaTopic + s.topicSuffix

	maxTokenIfNotExist := timber.Context().AppMaxTPS
	if s.limiter.IsHitLimit(topic, maxTokenIfNotExist) {
		onLimitExceeded(rw)
		return false, ""
	}

	if !s.admin.Exist(topic) {
		numPartitions := timber.Context().KafkaPartition
		replicationFactor := timber.Context().KafkaReplicationFactor

		log.Infof("%s is not exist. Creating topic with partition:%v replication_factor:%v", topic, numPartitions, replicationFactor)

		err = s.admin.CreateTopic(topic, numPartitions, replicationFactor)
		if err != nil {
			onCreateTopicError(rw, err)
			return false, ""
		}

		s.admin.AddTopic(topic)
		err = s.sendCreateTopicEvents(topic)
		if err != nil {
			onSendCreateTopicError(rw, err)
			return false, ""
		}
	}

	err = s.sendLogs(topic, timber)
	if err != nil {
		onStoreError(rw, err)
		return false, ""
	}

	return true, topic
}
