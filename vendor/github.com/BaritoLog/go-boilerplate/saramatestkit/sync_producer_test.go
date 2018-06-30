package saramatestkit

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
)

func TestNewSyncProduer(t *testing.T) {
	var producer sarama.SyncProducer = NewSyncProducer()

	partition, offset, err := producer.SendMessage(nil)
	FatalIf(t, partition != 0 || offset != 0 || err != nil, "wrong producer.SendMessage()")

	FatalIf(t, producer.SendMessages(nil) != nil, "wrong producer.SendMessages()")

	FatalIf(t, producer.Close() != nil, "wrong producer.Close()")

}
