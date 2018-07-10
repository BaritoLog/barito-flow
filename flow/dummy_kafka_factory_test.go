package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestDummyKafkaFactory(t *testing.T) {
	var v interface{} = NewDummyKafkaFactory()
	factory, ok := v.(KafkaFactory)

	FatalIf(t, !ok, "factory must be implement fo KafkaFactory")

	worker, err := factory.MakeConsumerWorker("groupID", "topic")
	FatalIf(t, worker != nil || err != nil, "MakeConsumerWorker return wrong value")

	admin, err := factory.MakeKafkaAdmin()
	FatalIf(t, admin != nil || err != nil, "MakeKafkaAdmin return wrong value")
}
