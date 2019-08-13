package flow

import (
	"net/http"
	"time"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

const (
	ErrMakeSyncProducer       = errkit.Error("Make sync producer failed")
	ErrKafkaRetryLimitReached = errkit.Error("Error connecting to kafka, retry limit reached")
)

type BaritoProducerService interface {
	Start() error
	Close()
	ServeHTTP(rw http.ResponseWriter, req *http.Request)
}

type baritoProducerService struct {
	factory                KafkaFactory
	addr                   string
	rateLimitResetInterval int
	topicSuffix            string
	kafkaMaxRetry          int
	kafkaRetryInterval     int
	newEventTopic          string

	producer sarama.SyncProducer
	admin    KafkaAdmin
	server   *http.Server
	limiter  RateLimiter
}

func NewBaritoProducerService(factory KafkaFactory, addr string, maxTps int, rateLimitResetInterval int, topicSuffix string, kafkaMaxRetry int, kafkaRetryInterval int, newEventTopic string) BaritoProducerService {
	return &baritoProducerService{
		factory:                factory,
		addr:                   addr,
		rateLimitResetInterval: rateLimitResetInterval,
		topicSuffix:            topicSuffix,
		kafkaMaxRetry:          kafkaMaxRetry,
		kafkaRetryInterval:     kafkaRetryInterval,
		newEventTopic:          newEventTopic,
	}
}

func (s *baritoProducerService) Start() (err error) {
	err = s.initProducer()
	if err != nil {
		err = errkit.Concat(ErrMakeSyncProducer, err)
		return
	}

	err = s.initKafkaAdmin()
	if err != nil {
		err = errkit.Concat(ErrMakeKafkaAdmin, err)
		return
	}

	s.limiter = NewRateLimiter(s.rateLimitResetInterval)
	s.limiter.Start()

	server := s.initHttpServer()
	return server.ListenAndServe()
}

func (s *baritoProducerService) initProducer() (err error) {
	finish := false
	retry := 0
	for !finish {
		retry += 1
		s.producer, err = s.factory.MakeSyncProducer()
		if err == nil {
			finish = true
			if retry > 1 {
				log.Infof("Retry kafka sync producer successful")
			}
		} else {
			if (s.kafkaMaxRetry == 0) || (retry < s.kafkaMaxRetry) {
				log.Warnf("Cannot connect to kafka: %s, retrying in %d seconds", err, s.kafkaRetryInterval)
				time.Sleep(time.Duration(s.kafkaRetryInterval) * time.Second)
			} else {
				err = ErrKafkaRetryLimitReached
				return
			}
		}
	}

	return
}

func (s *baritoProducerService) initKafkaAdmin() (err error) {
	finish := false
	retry := 0
	for !finish {
		retry += 1
		s.admin, err = s.factory.MakeKafkaAdmin()
		if err == nil {
			finish = true
			if retry > 1 {
				log.Infof("Retry initialize kafka admin successful")
			}
		} else {
			if (s.kafkaMaxRetry == 0) || (retry < s.kafkaMaxRetry) {
				log.Warnf("Cannot connect to kafka: %s, retrying in %d seconds", err, s.kafkaRetryInterval)
				time.Sleep(time.Duration(s.kafkaRetryInterval) * time.Second)
			} else {
				err = ErrKafkaRetryLimitReached
				return
			}
		}
	}

	return
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

		// add suffix
		topic = timberCollection.Context.KafkaTopic + s.topicSuffix

		maxTokenIfNotExist := int32(timberCollection.Context.AppMaxTPS)
		if s.limiter.IsHitLimit(topic, len(timberCollection.Items), maxTokenIfNotExist) {
			onLimitExceeded(rw)
			return
		}

		for _, timber := range timberCollection.Items {
			timber.SetContext(&timberCollection.Context)
			if timber.Timestamp() == "" {
				timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
			}

			success = s.handleProduce(rw, timber, topic)

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

		// add suffix
		topic = timber.Context().KafkaTopic + s.topicSuffix

		maxTokenIfNotExist := int32(timber.Context().AppMaxTPS)
		if s.limiter.IsHitLimit(topic, 1, maxTokenIfNotExist) {
			onLimitExceeded(rw)
			return
		}

		success = s.handleProduce(rw, timber, topic)
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

func (s *baritoProducerService) handleProduce(rw http.ResponseWriter, timber Timber, topic string) bool {
	var err error

	if !s.admin.Exist(topic) {
		numPartitions := timber.Context().KafkaPartition
		replicationFactor := timber.Context().KafkaReplicationFactor

		log.Warnf("%s is not exist. Creating topic with partition:%v replication_factor:%v", topic, numPartitions, replicationFactor)

		err = s.admin.CreateTopic(topic, numPartitions, replicationFactor)
		if err != nil {
			onCreateTopicError(rw, err)
			return false
		}

		s.admin.AddTopic(topic)
		err = s.sendCreateTopicEvents(topic)
		if err != nil {
			onSendCreateTopicError(rw, err)
			return false
		}
	}

	err = s.sendLogs(topic, timber)
	if err != nil {
		onStoreError(rw, err)
		return false
	}

	return true
}
