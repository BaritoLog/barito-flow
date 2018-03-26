package river

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestDownstreamKafka(t *testing.T) {

	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndSucceed()

	ds := KafkaDownstream{producer: producer}
	err := ds.Store(Timber{
		Location: "location",
		Message:  "some data",
	})

	FatalIfError(t, err)
}

func TestNewDownstreamKafka_Unreachable(t *testing.T) {
	conf := KafkaDownstreamConfig{
		Brokers:          []string{"stopped_kafka_broker"},
		ProducerRetryMax: 12,
	}
	_, err := NewKafkaDownstream(conf)
	FatalIfWrongError(t, err, "kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")
}

func TestNewDownstreamKafka_WrongTypeParameter(t *testing.T) {
	_, err := NewKafkaDownstream("meh")
	FatalIfWrongError(t, err, "Parameter must be KafkaDownstreamConfig")
}
