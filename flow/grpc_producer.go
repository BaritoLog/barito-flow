package flow

import (
	"context"

	pb "github.com/BaritoLog/barito-flow/proto"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
)

type baritoProducerServer struct {
	factory                KafkaFactory
	addr                   string
	rateLimitResetInterval int
	topicSuffix            string
	kafkaMaxRetry          int
	kafkaRetryInterval     int
	newEventTopic          string

	producer sarama.SyncProducer
	admin    KafkaAdmin
	limiter  RateLimiter
}

func NewBaritoProducerServer(factory KafkaFactory, addr string, maxTps int, rateLimitResetInterval int, topicSuffix string, kafkaMaxRetry int, kafkaRetryInterval int, newEventTopic string) pb.BaritoProducerServer {
	return &baritoProducerServer{
		factory:                factory,
		addr:                   addr,
		rateLimitResetInterval: rateLimitResetInterval,
		topicSuffix:            topicSuffix,
		kafkaMaxRetry:          kafkaMaxRetry,
		kafkaRetryInterval:     kafkaRetryInterval,
		newEventTopic:          newEventTopic,
	}
}

func (s *baritoProducerServer) Produce(_ context.Context, timber *pb.Timber) (resp *pb.ProduceResult, err error) {
	topic := timber.GetContext().GetKafkaTopic() + s.topicSuffix

	maxTokenIfNotExist := timber.GetContext().GetAppMaxTps()
	if s.limiter.IsHitLimit(topic, 1, maxTokenIfNotExist) {
		err = onLimitExceededGrpc()
		return
	}

	timber.Timestamp = ptypes.TimestampNow()
	err = s.handleProduce(timber, topic)
	if err != nil {
		return
	}

	resp = &pb.ProduceResult{
		Topic: topic,
	}
	return
}

func (s *baritoProducerServer) ProduceBatch(_ context.Context, timberCollection *pb.TimberCollection) (resp *pb.ProduceResult, err error) {
	topic := timberCollection.GetContext().GetKafkaTopic() + s.topicSuffix

	maxTokenIfNotExist := timberCollection.GetContext().GetAppMaxTps()
	if s.limiter.IsHitLimit(topic, len(timberCollection.GetItems()), maxTokenIfNotExist) {
		err = onLimitExceededGrpc()
		return
	}

	for _, timber := range timberCollection.GetItems() {
		timber.Context = timberCollection.GetContext()
		timber.Timestamp = ptypes.TimestampNow()

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

func (s *baritoProducerServer) sendLogs(topic string, timber *pb.Timber) (err error) {
	message := ConvertTimberToKafkaMessage(timber, topic)
	_, _, err = s.producer.SendMessage(message)
	return
}

func (s *baritoProducerServer) sendCreateTopicEvents(topic string) (err error) {
	message := &sarama.ProducerMessage{
		Topic: s.newEventTopic,
		Value: sarama.ByteEncoder(topic),
	}
	_, _, err = s.producer.SendMessage(message)
	return
}

func (s *baritoProducerServer) handleProduce(timber *pb.Timber, topic string) (err error) {
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
