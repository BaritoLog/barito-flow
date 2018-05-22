package cmds

import (
	"os"
	"strconv"

	"github.com/BaritoLog/barito-flow/flow"
	"github.com/Shopify/sarama"
	"github.com/containous/traefik/log"
	"github.com/urfave/cli"
)

const (
	EnvAddress                   = "BARITO_RECEIVER_ADDRESS"
	EnvKafkaBrokers              = "BARITO_RECEIVER_KAFKA_BROKERS"
	EnvKafkaTopic                = "BARITO_RECEIVER_KAFKA_TOPIC"
	EnvProducerMaxRetry          = "BARITO_RECEIVER_PRODUCER_MAX_RETRY"
	EnvReceiverApplicationSecret = "BARITO_RECEIVER_APPLICATION_SECRET"
)

func Receiver(c *cli.Context) (err error) {

	address := os.Getenv(EnvAddress)
	if address == "" {
		address = ":8080"
	}

	kafkaBrokers := os.Getenv(EnvKafkaBrokers)
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	producerMaxRetry, _ := strconv.Atoi(os.Getenv(EnvProducerMaxRetry))
	if producerMaxRetry <= 0 {
		producerMaxRetry = 10
	}

	kafkaTopic := os.Getenv(EnvKafkaTopic)
	if kafkaTopic == "" {
		kafkaTopic = "barito-log"
	}

	log.Infof("Start Receiver")
	log.Infof("%s=%s", EnvAddress, address)
	log.Infof("%s=%s", EnvKafkaBrokers, kafkaBrokers)
	log.Infof("%s=%s", EnvKafkaTopic, kafkaTopic)
	log.Infof("%s=%d", EnvProducerMaxRetry, producerMaxRetry)

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = producerMaxRetry
	config.Producer.Return.Successes = true

	// kafka producer
	producer, err := sarama.NewSyncProducer([]string{kafkaBrokers}, config)
	if err != nil {
		return
	}

	storeman := flow.NewKafkaStoreman(producer, kafkaTopic)
	agent := flow.HttpAgent{Address: address, Store: storeman.Store}

	return agent.Start()
}
