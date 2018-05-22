package cmds

import (
	"os"
	"strconv"
	"strings"

	"github.com/BaritoLog/barito-flow/river"
	"github.com/sirupsen/logrus"
)

const (
	EnvAddress                   = "BARITO_RECEIVER_ADDRESS"
	EnvKafkaBrokers              = "BARITO_RECEIVER_KAFKA_BROKERS"
	EnvKafkaTopic                = "BARITO_RECEIVER_KAFKA_TOPIC"
	EnvProducerMaxRetry          = "BARITO_RECEIVER_PRODUCER_MAX_RETRY"
	EnvReceiverApplicationSecret = "BARITO_RECEIVER_APPLICATION_SECRET"
)

type ReceiverConfig struct {
	Address           string
	KafkaBrokers      string
	KafkaTopic        string
	ProducerMaxRetry  int
	ApplicationSecret string
}

// NewReceiverConfigByEnv
func NewReceiverConfigByEnv() (*ReceiverConfig, error) {

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

	appSecret := os.Getenv(EnvReceiverApplicationSecret)
	if appSecret == "" {
		appSecret = "secret"
	}

	kafkaTopic := os.Getenv(EnvKafkaTopic)
	if kafkaTopic == "" {
		kafkaTopic = "barito-log"
	}

	config := &ReceiverConfig{
		Address:           address,
		ApplicationSecret: appSecret,
		KafkaBrokers:      kafkaBrokers,
		KafkaTopic:        kafkaTopic,
		ProducerMaxRetry:  producerMaxRetry,
	}

	return config, nil
}

// ReceiverUpstreamConfig
func (c ReceiverConfig) ReceiverUpstream() (river.Upstream, error) {
	return river.NewReceiverUpstream(river.ReceiverUpstreamConfig{
		Addr:      c.Address,
		AppSecret: c.ApplicationSecret,
	})
}

// KafkaDownstreamConfig
func (c ReceiverConfig) KafkaDownstream() (river.Downstream, error) {
	return river.NewKafkaDownstream(river.KafkaDownstreamConfig{
		Brokers:          strings.Split(c.KafkaBrokers, ","),
		ProducerRetryMax: c.ProducerMaxRetry,
		Topic:            c.KafkaTopic,
	})
}

func (c ReceiverConfig) Info(log *logrus.Logger) {
	log.Infof("%s=%s", EnvAddress, c.Address)
	log.Infof("%s=%s", EnvKafkaBrokers, c.KafkaBrokers)
	log.Infof("%s=%s", EnvKafkaTopic, c.KafkaTopic)
	log.Infof("%s=%d", EnvProducerMaxRetry, c.ProducerMaxRetry)
	log.Infof("%s=%s", EnvReceiverApplicationSecret, c.ApplicationSecret)
}
