package river

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestKafkaUpstream_New_WrongParameter(t *testing.T) {
	_, err := NewKafkaUpstream("meh")
	FatalIfWrongError(t, err, "Parameter must be KafkaUpstreamConfig")
}

func TestKafkaUpstream_StartTransport_WrongBroker(t *testing.T) {
	conf := KafkaUpstreamConfig{
		Brokers:         []string{"wrong_broker"},
		ConsumerGroupId: "some-consumer-group",
		ConsumerTopic:   []string{"some-topic"},
	}

	_, err := NewKafkaUpstream(conf)
	FatalIfWrongError(t, err, "kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")
}

func TestKafkaUpstream_SetErrorChannel(t *testing.T) {
	errCh := make(chan error)
	upstream := kafkaUpstream{}

	upstream.SetErrorChannel(errCh)
	FatalIf(t, errCh != upstream.ErrorChannel(), "ErrorChannel is return wrong channel")
}

func TestKafkaUpstream_TimberTunnel(t *testing.T) {
	timberCh := make(chan Timber)
	upstream := kafkaUpstream{timberCh: timberCh}
	FatalIf(t, timberCh != upstream.TimberChannel(), "TimberChannel is return wrong channel")
}
