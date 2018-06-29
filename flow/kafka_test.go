package flow

import (
	"fmt"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestKafkaStoreman_Store(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndSucceed()

	err := kafkaStore(producer, "some-topic", NewTimber())

	FatalIfError(t, err)
}

func TestKafkaStoreman_Store_ReturnError(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndFail(fmt.Errorf("some-error"))

	err := kafkaStore(producer, "some-topic", NewTimber())

	FatalIfWrongError(t, err, "some-error")
}
