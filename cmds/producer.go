package cmds

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/envkit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"

	"github.com/urfave/cli"
)

func Producer(c *cli.Context) (err error) {

	address := envkit.GetString(EnvProducerAddress, ":8080")
	kafkaBrokers := envkit.GetSlice(EnvKafkaBrokers, ",", []string{"localhost:9092"})
	producerMaxRetry := envkit.GetInt(EnvProducerMaxRetry, 10)
	kafkaTopic := envkit.GetString(EnvKafkaProducerTopic, "topic01")

	log.Infof("Start Producer")
	log.Infof("%s=%s", EnvProducerAddress, address)
	log.Infof("%s=%s", EnvKafkaBrokers, kafkaBrokers)
	log.Infof("%s=%s", EnvKafkaProducerTopic, kafkaTopic)
	log.Infof("%s=%d", EnvProducerMaxRetry, producerMaxRetry)

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

	agent := flow.HttpAgent{
		Address: address,
		Store:   flow.NewKafkaStoreman(producer, kafkaTopic).Store,
	}

	return agent.Start()
}
