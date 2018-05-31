package flow

import (
	"fmt"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestKafkaStoreman_New(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	topic := "some-topic"

	storeman := NewKafkaStoreman(producer, topic)

	kafkaStoreman, ok := storeman.(*kafkaStoreman)
	FatalIf(t, !ok, "wrong storeman")

	FatalIf(t, kafkaStoreman.producer != producer, "wrong producer")
	FatalIf(t, kafkaStoreman.topic != topic, "wrong topic")
}

func TestKafkaStoreman_Store(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndSucceed()

	storeman := NewKafkaStoreman(producer, "some-topic")
	err := storeman.Store(NewTimber())

	FatalIfError(t, err)
}

func TestKafkaStoreman_Store_ReturnError(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndFail(fmt.Errorf("some-error"))

	storeman := NewKafkaStoreman(producer, "some-topic")
	err := storeman.Store(NewTimber())

	FatalIfWrongError(t, err, "some-error")
}
