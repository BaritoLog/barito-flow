package cmds

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/srvkit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"

	"github.com/urfave/cli"
)

func Producer(c *cli.Context) (err error) {

	log.Infof("Start Barito-Producer")

	address := getProducerAddress()
	kafkaBrokers := getKafkaBrokers()
	maxRetry := getProducerMaxRetry()
	kafkaProducerTopic := getKafkaProducerTopic()
	producerMaxTps := getProducerMaxTPS()

	// TODO: move info to config source file
	log.Infof("ProducerAddress: %s", address)
	log.Infof("KafkaBrokers: %s", kafkaBrokers)
	log.Infof("KafkaProducerTopic: %s", kafkaProducerTopic)
	log.Infof("ProducerMaxRetry: %d", maxRetry)
	log.Infof("ProducerMaxTps: %d", producerMaxTps)

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true

	// kafka producer
	kafkaProducer, err := sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		return
	}

	// TODO: get topicSuffix
	srv := flow.NewBaritoProducerService(address, kafkaProducer, producerMaxTps, "_logs")
	srvkit.AsyncGracefulShutdown(srv.Close)

	return srv.Start()
}
