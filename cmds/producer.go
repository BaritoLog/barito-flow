package cmds

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"

	"github.com/urfave/cli"
)

func Producer(c *cli.Context) (err error) {

	producerAddress := getProducerAddress()
	kafkaBrokers := getKafkaBrokers()
	producerMaxRetry := getProducerMaxRetry()
	kafkaProducerTopic := getKafkaProducerTopic()
	producerMaxTps := getProducerMaxTPS()

	log.Infof("[Start Producer]")
	log.Infof("ProducerAddress: %s", producerAddress)
	log.Infof("KafkaBrokers: %s", kafkaBrokers)
	log.Infof("KafkaProducerTopic: %s", kafkaProducerTopic)
	log.Infof("ProducerMaxRetry: %d", producerMaxRetry)
	log.Infof("ProducerMaxTps: %d", producerMaxTps)

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = producerMaxRetry
	config.Producer.Return.Successes = true

	// kafka producer
	producer, err := sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		return
	}

	agent := flow.NewHttpAgent(
		producerAddress,
		flow.NewKafkaStoreman(producer, kafkaProducerTopic).Store,
		producerMaxTps,
	)

	return agent.Start()
}
