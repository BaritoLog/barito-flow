package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
)

func TestDummyKafkaFactory(t *testing.T) {
	var v interface{} = NewDummyKafkaFactory()
	factory, ok := v.(KafkaFactory)

	FatalIf(t, !ok, "factory must be implement fo KafkaFactory")

	consumer, err := factory.MakeClusterConsumer("groupID", "topic", sarama.OffsetNewest)
	FatalIf(t, consumer != nil || err != nil, "MakeClusterConsumer return wrong value")

	admin, err := factory.MakeKafkaAdmin()
	FatalIf(t, admin != nil || err != nil, "MakeKafkaAdmin return wrong value")
}
