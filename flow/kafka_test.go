package flow

import (
	"fmt"
	"testing"

	"github.com/BaritoLog/go-boilerplate/saramatestkit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestKafkaStoreman_Store(t *testing.T) {
	var topic string

	dummy := saramatestkit.NewSyncProducer()
	dummy.SendMessageFunc = func(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
		topic = msg.Topic
		return
	}

	timber := NewTimber()
	timber.SetContext(&TimberContext{
		KafkaTopic: "some-topic",
	})

	err := kafkaStore(dummy, timber, "_logs")

	FatalIfError(t, err)
	FatalIf(t, topic != "some-topic_logs", "wrong topic")
}

func TestKafkaStoreman_Store_ReturnError(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndFail(fmt.Errorf("some-error"))

	timber := NewTimber()
	timber.SetContext(&TimberContext{
		KafkaTopic: "some-topic",
	})

	err := kafkaStore(producer, timber, "_logs")

	FatalIfWrongError(t, err, "some-error")
}
