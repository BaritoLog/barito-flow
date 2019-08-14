package flow

import (
	"context"
	"net"
	"net/http"
	"time"

	pb "github.com/BaritoLog/barito-flow/proto"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	ErrMakeSyncProducer       = errkit.Error("Make sync producer failed")
	ErrKafkaRetryLimitReached = errkit.Error("Error connecting to kafka, retry limit reached")
	ErrInitGrpc               = errkit.Error("Failed to listen to gRPC address")
	ErrRegisterGrpc           = errkit.Error("Error registering gRPC server endpoint into reverse proxy")
	ErrReverseProxy           = errkit.Error("Error serving REST reverse proxy")
)

type ProducerService interface {
	pb.ProducerServiceServer
	Start() error
	LaunchREST() error
	Close()
}

type producerService struct {
	factory                KafkaFactory
	grpcAddr               string
	restAddr               string
	rateLimitResetInterval int
	topicSuffix            string
	kafkaMaxRetry          int
	kafkaRetryInterval     int
	newEventTopic          string

	producer sarama.SyncProducer
	admin    KafkaAdmin
	limiter  RateLimiter

	grpcServer   *grpc.Server
	reverseProxy *http.Server
}

func NewProducerService(params map[string]interface{}) ProducerService {
	return &producerService{
		factory:                params["factory"].(KafkaFactory),
		grpcAddr:               params["grpcAddr"].(string),
		restAddr:               params["restAddr"].(string),
		rateLimitResetInterval: params["rateLimitResetInterval"].(int),
		topicSuffix:            params["topicSuffix"].(string),
		kafkaMaxRetry:          params["kafkaMaxRetry"].(int),
		kafkaRetryInterval:     params["kafkaRetryInterval"].(int),
		newEventTopic:          params["newEventTopic"].(string),
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

	srv = grpc.NewServer()
	pb.RegisterProducerServiceServer(srv, s)

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

	s.limiter = NewRateLimiter(s.rateLimitResetInterval)
	s.limiter.Start()

	lis, grpcSrv, err := s.initGrpcServer()
	if err != nil {
		err = errkit.Concat(ErrInitGrpc, err)
		return
	}

	return grpcSrv.Serve(lis)
}

func (s *producerService) LaunchREST() (err error) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err = pb.RegisterProducerServiceHandlerFromEndpoint(ctx, mux, "localhost"+s.grpcAddr, opts)
	if err != nil {
		err = errkit.Concat(ErrRegisterGrpc, err)
		return
	}

	s.reverseProxy = &http.Server{
		Addr:    s.restAddr,
		Handler: mux,
	}

	err = s.reverseProxy.ListenAndServe()
	if err != nil {
		err = errkit.Concat(ErrReverseProxy, err)
	}
	return
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
	topic := timber.GetContext().GetKafkaTopic() + s.topicSuffix

	maxTokenIfNotExist := timber.GetContext().GetAppMaxTps()
	if s.limiter.IsHitLimit(topic, 1, maxTokenIfNotExist) {
		err = onLimitExceededGrpc()
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
	topic := timberCollection.GetContext().GetKafkaTopic() + s.topicSuffix

	maxTokenIfNotExist := timberCollection.GetContext().GetAppMaxTps()
	if s.limiter.IsHitLimit(topic, len(timberCollection.GetItems()), maxTokenIfNotExist) {
		err = onLimitExceededGrpc()
		return
	}

	for _, timber := range timberCollection.GetItems() {
		timber.Context = timberCollection.GetContext()
		timber.Timestamp = time.Now().UTC().Format(time.RFC3339)

		err = s.handleProduce(timber, topic)
		if err != nil {
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
	_, _, err = s.producer.SendMessage(message)
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
		numPartitions := timber.GetContext().GetKafkaPartition()
		replicationFactor := timber.GetContext().GetKafkaReplicationFactor()

		log.Warnf("%s does not exist. Creating topic with partition:%v replication_factor:%v", topic, numPartitions, replicationFactor)

		err = s.admin.CreateTopic(topic, numPartitions, int16(replicationFactor))
		if err != nil {
			err = onCreateTopicErrorGrpc(err)
			return
		}

		s.admin.AddTopic(topic)
		err = s.sendCreateTopicEvents(topic)
		if err != nil {
			err = onSendCreateTopicErrorGrpc(err)
			return
		}
	}

	err = s.sendLogs(topic, timber)
	if err != nil {
		err = onStoreErrorGrpc(err)
		return
	}

	return
}
