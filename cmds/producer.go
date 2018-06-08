package cmds

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"

	"github.com/urfave/cli"
)

func Producer(c *cli.Context) (err error) {

	address := getProducerAddress()
	kafkaBrokers := getKafkaBrokers()
	producerMaxRetry := getProducerMaxRetry()
	kafkaTopic := getKafkaProducerTopic()
	maxTps := getProducerMaxTPS()

	log.Infof("Start Producer")
	log.Infof("%s=%s", EnvProducerAddress, address)
	log.Infof("%s=%s", EnvKafkaBrokers, kafkaBrokers)
	log.Infof("%s=%s", EnvKafkaProducerTopic, kafkaTopic)
	log.Infof("%s=%d", EnvProducerMaxRetry, producerMaxRetry)
	log.Infof("%s=%d", EnvProducerMaxTPS, maxTps)

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
		address,
		flow.NewKafkaStoreman(producer, kafkaTopic).Store,
		maxTps,
	)

	return agent.Start()
}
