package kafka

import (
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"testing"
)

var server = Server{
	brokers: "localhost:9092",
}

func TestNewKafka(t *testing.T) {
	kafka := NewKafka(server.brokers)

	FatalIf(t, kafka == nil, "Kafka is nil")
}
