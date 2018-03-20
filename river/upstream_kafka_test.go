package river

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestKafkaUpstream_WrongParameter(t *testing.T) {
	_, err := NewKafkaUpstream("meh")
	FatalIfWrongError(t, err, "Parameter must be KafkaUpstreamConfig")
}

func TestKafkaUPstream_StartTransport_WrongBroker(t *testing.T) {
	conf := KafkaUpstreamConfig{
		Brokers:         []string{"wrong_broker"},
		ConsumerGroupId: "some-consumer-group",
		ConsumerTopic:   []string{"some-topic"},
	}

	kafka, err := NewKafkaUpstream(conf)
	FatalIfError(t, err)

	go kafka.StartTransport()
	timekit.Sleep("100ms")

	err = <-kafka.ErrorChannel()

	FatalIfWrongError(t, err, "kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")

}
