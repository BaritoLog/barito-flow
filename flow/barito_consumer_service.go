package flow

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
)

type BaritoConsumerService interface {
	Start() error
	Close()
}

type baritoConsumerService struct {
	KafkaBrokers []string
	KafkaGroupID string
	ElasticUrl   string
	TopicSuffix  string
	workers      map[string]ConsumerWorker
	config       *sarama.Config
}

func NewBaritoConsumerService(kafkaBrokers []string, kafkaGroupID, elasticURL, topicSuffix string) BaritoConsumerService {

	// TODO:
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0

	return &baritoConsumerService{
		KafkaBrokers: kafkaBrokers,
		KafkaGroupID: kafkaGroupID,
		ElasticUrl:   elasticURL,
		TopicSuffix:  topicSuffix,
		workers:      make(map[string]ConsumerWorker),
		config:       config,
	}
}

func (s baritoConsumerService) Start() (err error) {

	log.Infof("Start Barito Consumer Service")
	admin, err := NewKafkaAdmin(s.KafkaBrokers, s.config)
	if err != nil {
		return
	}

	topics := admin.TopicsWithSuffix(s.TopicSuffix)
	admin.Close()

	for _, topic := range topics {
		log.Infof("Spawn new worker for topic '%s'", topic)
		var worker ConsumerWorker
		worker, err = s.spawnNewWorker(topic)
		if err != nil {
			return
		}

		// TODO: worker's instrumentation

		s.workers[topic] = worker
		go worker.Start()
	}
	return
}

func (s baritoConsumerService) Close() {
	for _, worker := range s.workers {
		worker.Close()
	}
}

func (s baritoConsumerService) spawnNewWorker(topic string) (worker ConsumerWorker, err error) {

	// elastic client
	client, err := elasticNewClient(s.ElasticUrl)

	// consumer config
	config := cluster.NewConfig()
	config.Config = *s.config
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// kafka consumer
	consumer, err := cluster.NewConsumer(s.KafkaBrokers, s.KafkaGroupID,
		[]string{topic}, config)
	if err != nil {
		return
	}

	worker = NewConsumerWorker(consumer, client)
	worker.OnError(s.onErrror)
	return
}

func (s baritoConsumerService) onErrror(err error) {
	log.Warn(err.Error())
}
