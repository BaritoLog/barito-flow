package flow

import (
	"fmt"
	"regexp"
	"time"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/prome"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

type KafkaConsumerVLogsSettings struct {
	GroupIDPrefix string `envconfig:"kafka_group_id_prefix" required:"true"` // consumer_group name
	TopicPattern  string `envconfig:"kafka_topic_pattern" required:"true"`   // topic pattern to be consumed, regex
}

type baritoKafkaConsumerVLogsService struct {
	groupIDPrefix     string
	topicPatternRegex regexp.Regexp

	workerMap      map[string]types.ConsumerWorker
	VLogsOutputMap map[string]types.ConsumerOutput

	kafkaAdmin           types.KafkaAdmin
	kafkaFactory         types.KafkaFactory
	consumerOuputFactory types.ConsumerOutputFactory
	marshaler            *jsonpb.Marshaler

	logger *log.Entry
	isStop bool
}

func NewBaritoKafkaConsumerVLogsFromEnv(kafkaFactory types.KafkaFactory, consumerOutputFactory types.ConsumerOutputFactory) BaritoConsumerService {
	settings := KafkaConsumerVLogsSettings{}
	envconfig.MustProcess("", &settings)

	var err error
	var kafkaAdmin types.KafkaAdmin
	for {
		kafkaAdmin, err = kafkaFactory.MakeKafkaAdmin()
		if err != nil {
			log.Error("Error creating kafka admin: ", err)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	s := &baritoKafkaConsumerVLogsService{
		groupIDPrefix:        settings.GroupIDPrefix,
		topicPatternRegex:    *regexp.MustCompile(settings.TopicPattern),
		kafkaAdmin:           kafkaAdmin,
		kafkaFactory:         kafkaFactory,
		consumerOuputFactory: consumerOutputFactory,
		workerMap:            make(map[string]types.ConsumerWorker),
		VLogsOutputMap:       make(map[string]types.ConsumerOutput),
		marshaler:            &jsonpb.Marshaler{},
		logger:               log.New().WithField("component", "BaritoKafkaConsumerVLogs"),
	}
	return s
}

func (s *baritoKafkaConsumerVLogsService) Start() error {
	s.logger.Warn("Start Barito Kafka Consumer VLogs Service")
	go func() {
		for {
			if s.isStop {
				break
			}

			err := s.kafkaAdmin.RefreshTopics()
			if err != nil {
				prome.IncreaseConsumerCustomErrorTotal("kafka_admin_refresh_topics")
				s.logger.Error(err)
			}
			for _, topic := range s.kafkaAdmin.Topics() {
				if !s.topicPatternRegex.MatchString(topic) {
					s.logger.Debug("Topic doesn't match pattern", topic)
					continue // skip, didn't match topic pattern
				}

				if _, ok := s.workerMap[topic]; ok {
					s.logger.Debug("Topic already being handled", topic)
					continue // skip, already being handled
				}
				s.logger.Warn("Handling topic ", topic)

				// initiate kafka consumer and VLogs types.ConsumerOutput
				err := s.spawnLogsWorker(topic, sarama.OffsetOldest)
				if err != nil {
					prome.IncreaseConsumerCustomErrorTotalf("spawn_logs_worker_%s", topic)
					s.logger.WithField("topic", topic).Error(err)
					continue
				}
			}

			time.Sleep(1 * time.Minute)
		}
	}()

	return nil
}

func (s *baritoKafkaConsumerVLogsService) spawnLogsWorker(topic string, initialOffset int64) (err error) {
	// create VLogs types.ConsumerOutput
	g, err := s.consumerOuputFactory.MakeConsumerOutputVLogs(topic)
	if err != nil {
		err = fmt.Errorf("error creating VLogs output: %s", err)
		s.logger.WithField("topic", topic).Error(err)
		return err
	}
	s.VLogsOutputMap[topic] = g

	// consumerggroup name, is prefix + _ + topic
	groupID := s.groupIDPrefix + topic

	// create or get kafka consumer
	consumer, err := s.kafkaFactory.MakeClusterConsumer(groupID, topic, initialOffset)
	if err != nil {
		err = errkit.Concat(ErrConsumerWorker, err)
		s.logger.WithField("topic", topic).Error(err)
		return err
	}
	worker := s.kafkaFactory.MakeConsumerWorker(topic, consumer)
	s.workerMap[topic] = worker

	// log when on error
	worker.OnError(func(err error) {
		s.logger.WithField("topic", topic).Error(err)
	})

	// when there are new message, call the VLogs OnMessage
	worker.OnSuccess(func(msg *sarama.ConsumerMessage) {
		timber, err := ConvertKafkaMessageToTimber(msg)
		if err != nil {
			err = errkit.Concat(ErrConvertKafkaMessage, err)
			s.logger.WithField("topic", topic).Error(err)
			return
		}

		content, err := ConvertTimberToVlogsJson(&timber, s.marshaler)
		if err != nil {
			s.logger.WithField("topic", topic).Error(err)
			return
		}

		// retry indefinitely until success
		// the buffer might be full, or there is still inflight request
		for {
			err := g.OnMessage([]byte(content))
			if err == nil {
				break
			}
			prome.IncreaseConsumerCustomErrorTotalf("VLogs_on_message: %s", topic)
			time.Sleep(500 * time.Millisecond)
		}
	})

	// setup hook when the VLogsService flush, it will commit the offset to the kafka
	g.AddOnFlushFunc(func() error {
		err := worker.OnConsumerFlush()
		return err
	})

	worker.Start()
	go g.Start()

	return
}

func (s *baritoKafkaConsumerVLogsService) Close() {
	s.isStop = true

	for _, g := range s.VLogsOutputMap {
		g.Stop()
	}

	// FIXME: currently the worker will autocommit the offset when it's stopped
	//for _, w := range s.workerMap {
	//w.Stop()
	//}
}

func (s *baritoKafkaConsumerVLogsService) WorkerMap() map[string]types.ConsumerWorker {
	return s.workerMap
}

func (s *baritoKafkaConsumerVLogsService) NewTopicEventWorker() types.ConsumerWorker {
	// irrelevant, this consumer didn't use new_topic_event
	return nil
}

func (s *baritoKafkaConsumerVLogsService) keepRefreshKafkaTopics() {
	for {
		err := s.kafkaAdmin.RefreshTopics()
		if err != nil {
			log.Warn(err)
		}
		time.Sleep(1 * time.Minute)
	}
}
