package flow

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/prome"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	pb "github.com/bentol/barito-proto/producer"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	_ "github.com/mostynb/go-grpc-compression/zstd"
)

const (
	ErrMakeSyncProducer       = errkit.Error("Make sync producer failed")
	ErrKafkaRetryLimitReached = errkit.Error("Error connecting to kafka, retry limit reached")
	ErrInitGrpc               = errkit.Error("Failed to listen to gRPC address")
	ErrRegisterGrpc           = errkit.Error("Error registering gRPC server endpoint into reverse proxy")

	RateLimitKeyAppGroup = "app_group"
)

type ProducerService interface {
	Start() error
	Close()
}

type producerService struct {
	pb.UnimplementedProducerServer
	factory            types.KafkaFactory
	grpcAddr           string
	topicPrefix        string
	topicSuffix        string
	kafkaMaxRetry      int
	kafkaRetryInterval int
	newEventTopic      string
	grpcMaxRecvMsgSize int
	ignoreKafkaOptions bool

	producer sarama.SyncProducer
	admin    types.KafkaAdmin
	limiter  RateLimiter

	grpcServer   *grpc.Server
	reverseProxy *http.Server
}

func NewProducerService(params map[string]interface{}) *producerService {
	return &producerService{
		UnimplementedProducerServer: pb.UnimplementedProducerServer{},
		factory:                     params["factory"].(types.KafkaFactory),
		grpcAddr:                    params["grpcAddr"].(string),
		topicPrefix:                 params["topicPrefix"].(string),
		topicSuffix:                 params["topicSuffix"].(string),
		kafkaMaxRetry:               params["kafkaMaxRetry"].(int),
		kafkaRetryInterval:          params["kafkaRetryInterval"].(int),
		newEventTopic:               params["newEventTopic"].(string),
		grpcMaxRecvMsgSize:          params["grpcMaxRecvMsgSize"].(int),
		ignoreKafkaOptions:          params["ignoreKafkaOptions"].(bool),
		limiter:                     params["limiter"].(RateLimiter),
	}
}

func (s *producerService) initProducer() (err error) {
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
			prome.IncreaseProducerKafkaClientFailed()
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

func (s *producerService) initKafkaAdmin() (err error) {
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

func (s *producerService) initGrpcServer() (lis net.Listener, srv *grpc.Server, err error) {
	lis, err = net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return
	}

	srv = grpc.NewServer(grpc.MaxRecvMsgSize(s.grpcMaxRecvMsgSize))
	pb.RegisterProducerServer(srv, s)

	s.grpcServer = srv
	return
}

func (s *producerService) Start() (err error) {
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

	s.limiter.Start()

	lis, grpcSrv, err := s.initGrpcServer()
	if err != nil {
		err = errkit.Concat(ErrInitGrpc, err)
		return
	}

	return grpcSrv.Serve(lis)
}

func (s *producerService) Close() {
	if s.reverseProxy != nil {
		s.reverseProxy.Close()
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.limiter != nil {
		s.limiter.Stop()
	}

	if s.admin != nil {
		s.admin.Close()
	}

	if s.producer != nil {
		s.producer.Close()
	}
}

func (s *producerService) Produce(_ context.Context, timber *pb.Timber) (resp *pb.ProduceResult, err error) {
	topic := s.topicPrefix + timber.GetContext().GetKafkaTopic() + s.topicSuffix
	rateLimitKey, maxToken := s.getRateLimitInfo(timber.GetContext())

	if s.limiter.IsHitLimit(rateLimitKey, 1, maxToken) {
		err = onLimitExceededGrpc()
		prome.IncreaseProducerTPSExceededCounter(topic, 1)

		timber.Timestamp = time.Now().UTC().Format(time.RFC3339)
		prome.ObserveTPSExceededBytes(topic, s.topicSuffix, timber)
		return
	}

	timber.Timestamp = time.Now().UTC().Format(time.RFC3339)
	err = s.handleProduce(timber, topic)
	if err != nil {
		return
	}

	resp = &pb.ProduceResult{
		Topic: topic,
	}
	return
}

func (s *producerService) ProduceBatch(_ context.Context, timberCollection *pb.TimberCollection) (resp *pb.ProduceResult, err error) {
	topic := s.topicPrefix + timberCollection.GetContext().GetKafkaTopic() + s.topicSuffix
	rateLimitKey, maxToken := s.getRateLimitInfo(timberCollection.GetContext())

	lengthMessages := len(timberCollection.GetItems())
	if s.limiter.IsHitLimit(rateLimitKey, lengthMessages, maxToken) {
		err = onLimitExceededGrpc()
		prome.IncreaseProducerTPSExceededCounter(topic, lengthMessages)

		for _, timber := range timberCollection.GetItems() {
			timber.Context = timberCollection.GetContext()
			timber.Timestamp = time.Now().UTC().Format(time.RFC3339)
			prome.ObserveTPSExceededBytes(topic, s.topicSuffix, timber)
		}
		return
	}

	for _, timber := range timberCollection.GetItems() {
		timber.Context = timberCollection.GetContext()
		timber.Timestamp = time.Now().UTC().Format(time.RFC3339)

		err = s.handleProduce(timber, topic)
		if err != nil {
			log.Infof("Failed send logs to kafka: %s", err)
			return
		}
	}

	resp = &pb.ProduceResult{
		Topic: topic,
	}
	return
}

func (s *producerService) sendLogs(topic string, timber *pb.Timber) (err error) {
	message := ConvertTimberToKafkaMessage(timber, topic)

	startTime := time.Now()
	_, _, err = s.producer.SendMessage(message)
	timeDifference := float64(time.Now().Sub(startTime).Nanoseconds()) / float64(1000000000)
	prome.ObserveSendToKafkaTime(topic, timeDifference)
	return
}

func (s *producerService) sendCreateTopicEvents(topic string) (err error) {
	message := &sarama.ProducerMessage{
		Topic: s.newEventTopic,
		Value: sarama.ByteEncoder(topic),
	}
	_, _, err = s.producer.SendMessage(message)
	return
}

func (s *producerService) handleProduce(timber *pb.Timber, topic string) (err error) {
	if !s.admin.Exist(topic) {
		var numPartitions int32 = -1
		var replicationFactor int32 = -1

		if !s.ignoreKafkaOptions {
			numPartitions = timber.GetContext().GetKafkaPartition()
			replicationFactor = timber.GetContext().GetKafkaReplicationFactor()
		}

		log.Warnf("%s does not exist. Creating topic with partition:%v replication_factor:%v", topic, numPartitions, replicationFactor)

		err = s.admin.CreateTopic(topic, numPartitions, int16(replicationFactor))
		if err != nil {
			err = onCreateTopicErrorGrpc(err)
			prome.IncreaseKafkaMessagesStoredTotalWithError(topic, "create_topic")
			return
		}

		s.admin.AddTopic(topic)
		err = s.sendCreateTopicEvents(topic)
		if err != nil {
			err = onSendCreateTopicErrorGrpc(err)
			prome.IncreaseKafkaMessagesStoredTotalWithError(topic, "send_create_topic_event")
			return
		}
	}

	err = s.sendLogs(topic, timber)
	if err != nil {
		err = onStoreErrorGrpc(err)
		prome.IncreaseKafkaMessagesStoredTotalWithError(topic, "send_log")
		return
	}
	prome.ObserveByteIngestion(topic, s.topicSuffix, timber)

	prome.IncreaseKafkaMessagesStoredTotal(topic)
	return
}

func (s *producerService) getRateLimitInfo(context *pb.TimberContext) (string, int32) {
	if context.GetDisableAppTps() {
		return RateLimitKeyAppGroup, context.GetAppGroupMaxTps()
	} else {
		return s.topicPrefix + context.GetKafkaTopic() + s.topicSuffix, context.GetAppMaxTps()
	}
}
