package flow

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/prome"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	uuid "github.com/gofrs/uuid"
	log "github.com/sirupsen/logrus"
	pb "github.com/vwidjaya/barito-proto/producer"
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

	PrefixEventGroupID          = "nte"
	TimberConvertErrorIndexName = "no_index"
)

type BaritoConsumerService interface {
	Start() error
	Close()
	WorkerMap() map[string]types.ConsumerWorker
	NewTopicEventWorker() types.ConsumerWorker
}

type baritoConsumerService struct {
	factory            types.KafkaFactory
	groupID            string
	uniqueGroupID      bool
	elasticUrls        []string
	esClients          []*elasticClient
	topicPrefix        string
	topicSuffix        string
	kafkaMaxRetry      int
	kafkaRetryInterval int
	newTopicEventName  string

	workerMap           map[string]types.ConsumerWorker
	admin               types.KafkaAdmin
	newTopicEventWorker types.ConsumerWorker
	eventWorkerGroupID  string

	lastError              error
	lastTimber             pb.Timber
	lastNewTopic           string
	isHalt                 bool
	elasticRetrierInterval string
	elasticRetrierMaxRetry int

	elasticUsername string
	elasticPassword string
}

func NewBaritoConsumerService(params map[string]interface{}) BaritoConsumerService {
	s := &baritoConsumerService{
		factory:                params["factory"].(types.KafkaFactory),
		groupID:                params["groupID"].(string),
		uniqueGroupID:          params["uniqueGroupID"].(bool),
		elasticUrls:            params["elasticUrls"].([]string),
		topicPrefix:            params["topicPrefix"].(string),
		topicSuffix:            params["topicSuffix"].(string),
		kafkaMaxRetry:          params["kafkaMaxRetry"].(int),
		kafkaRetryInterval:     params["kafkaRetryInterval"].(int),
		newTopicEventName:      params["newTopicEventName"].(string),
		workerMap:              make(map[string]types.ConsumerWorker),
		elasticRetrierInterval: params["elasticRetrierInterval"].(string),
		elasticRetrierMaxRetry: params["elasticRetrierMaxRetry"].(int),
		elasticUsername:        params["elasticUsername"].(string),
		elasticPassword:        params["elasticPassword"].(string),
	}

	httpClient := &http.Client{}
	// if using mTLS, create new http client with tls config
	if _, ok := params["elasticCaCrt"]; ok {
		httpClient = s.newHttpClientWithTLS(params["elasticCaCrt"].(string), params["elasticClientCrt"].(string), params["elasticClientKey"].(string))
	}

	s.esClients = []*elasticClient{}
	for i := 0; i < params["esNumWorker"].(int); i++ {
		retrier := s.elasticRetrier()
		esConfig := params["esConfig"].(esConfig)
		elastic, err := NewElastic(retrier, esConfig, s.elasticUrls, s.elasticUsername, s.elasticPassword, httpClient)
		s.esClients = append(s.esClients, &elastic)
		if err != nil {
			s.logError(errkit.Concat(ErrElasticsearchClient, err))
			prome.IncreaseConsumerElasticsearchClientFailed(prome.ESClientFailedPhaseInit)
		}
	}

	return s
}

func (s *baritoConsumerService) newHttpClientWithTLS(caCrt, clientCrt, clientKey string) *http.Client {
	caCert, _ := os.ReadFile(caCrt)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	cert, err := tls.LoadX509KeyPair(clientCrt, clientKey)
	if err != nil {
		s.logError(errkit.Concat(errors.New("Failed to create http client with tls"), err))
		panic(err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      caCertPool,
				Certificates: []tls.Certificate{cert},
			},
		},
	}

	return client
}

func (s *baritoConsumerService) Start() (err error) {

	admin, err := s.initAdmin()
	if err != nil {
		err = errkit.Concat(ErrMakeKafkaAdmin, err)
		return
	}

	uuid, _ := uuid.NewV4()
	s.eventWorkerGroupID = fmt.Sprintf("%s-%s", PrefixEventGroupID, uuid)
	log.Infof("Generate event worker group id: %s", s.eventWorkerGroupID)

	worker, err := s.initNewTopicWorker(s.eventWorkerGroupID)
	if err != nil {
		err = errkit.Concat(ErrMakeNewTopicWorker, err)
		s.logError(err)
		prome.IncreaseConsumerTimberConvertError(TimberConvertErrorIndexName)
		return
	}

	worker.Start()

	for _, topic := range admin.Topics() {
		if strings.HasPrefix(topic, s.topicPrefix) && strings.HasSuffix(topic, s.topicSuffix) {
			err := s.spawnLogsWorker(topic, sarama.OffsetNewest)
			if err != nil {
				s.logError(errkit.Concat(ErrSpawnWorker, err))
				prome.IncreaseConsumerTimberConvertError(topic + "_" + ErrSpawnWorker.Error())
				s.Close()
			}
		}
	}

	return
}

func (s *baritoConsumerService) initAdmin() (admin types.KafkaAdmin, err error) {
	finish := false
	retry := 0
	for !finish {
		retry += 1
		admin, err = s.factory.MakeKafkaAdmin()
		if err == nil {
			s.admin = admin
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

func (s *baritoConsumerService) initNewTopicWorker(groupID string) (worker types.ConsumerWorker, err error) { // TODO: return worker
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
	groupID := s.groupID
	if s.uniqueGroupID {
		groupID = fmt.Sprintf("%s_%s", s.groupID, topic)
	}
	consumer, err := s.factory.MakeClusterConsumer(groupID, topic, initialOffset)
	if err != nil {
		return errkit.Concat(ErrConsumerWorker, err)
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

func (s *baritoConsumerService) logTimber(timber pb.Timber) {
	s.lastTimber = timber
	log.Infof("Timber: %v", timber)
}

func (s *baritoConsumerService) logNewTopic(topic string) {
	s.lastNewTopic = topic
	log.Warnf("New topic: %s", topic)
}

func (s *baritoConsumerService) onElasticRetry(err error) {
	s.logError(errkit.Concat(ErrElasticsearchClient, err))
	prome.IncreaseConsumerElasticsearchClientFailed(prome.ESClientFailedPhaseRetry)
}

func (s *baritoConsumerService) onElasticMaxRetryReached() {
	s.HaltAllWorker()
}

func (s *baritoConsumerService) onStoreTimber(message *sarama.ConsumerMessage) {
	// convert kafka message
	timber, err := ConvertKafkaMessageToTimber(message)
	if err != nil {
		s.logError(errkit.Concat(ErrConvertKafkaMessage, err))
		return
	}

	// store to elasticsearch
	ctx := context.Background()
	s.getRandomElasticClient().Store(ctx, timber)
	if err != nil {
		s.logError(errkit.Concat(ErrStore, err))
		return
	}

	s.logTimber(timber)
}

func (s *baritoConsumerService) getRandomElasticClient() *elasticClient {
	randomIndex := time.Now().UnixNano() % int64(len(s.esClients))
	return s.esClients[randomIndex]
}

func (s *baritoConsumerService) onNewTopicEvent(message *sarama.ConsumerMessage) {
	topic := string(message.Value)

	_, ok := s.workerMap[topic]
	if ok {
		return
	}

	if !strings.HasPrefix(topic, s.topicPrefix) {
		return
	}

	err := s.spawnLogsWorker(topic, sarama.OffsetOldest)

	if err != nil {
		err = errkit.Concat(ErrSpawnWorkerOnNewTopic, err)
		s.logError(err)
		prome.IncreaseConsumerTimberConvertError(topic + "_" + ErrSpawnWorkerOnNewTopic.Error())
		s.Close()
		return
	}

	s.logNewTopic(topic)
}

func (s *baritoConsumerService) WorkerMap() map[string]types.ConsumerWorker {
	return s.workerMap
}

func (s *baritoConsumerService) NewTopicEventWorker() types.ConsumerWorker {
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
	return NewElasticRetrier(
		timekit.Duration(s.elasticRetrierInterval),
		s.elasticRetrierMaxRetry,
		s.onElasticRetry,
		s.onElasticMaxRetryReached,
	)
}

func (s *baritoConsumerService) ResumeWorker() (err error) {
	s.isHalt = false
	err = s.Start()

	return
}
