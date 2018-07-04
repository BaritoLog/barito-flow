package cmds

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/srvkit"
	"github.com/Shopify/sarama"

	"github.com/urfave/cli"
)

func Producer(c *cli.Context) (err error) {

	address := configProducerAddress()
	kafkaBrokers := configKafkaBrokers()
	maxRetry := configProducerMaxRetry()
	producerMaxTps := configProducerMaxTPS()

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
