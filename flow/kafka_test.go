package flow

import (
	"fmt"
	"testing"

	"github.com/BaritoLog/go-boilerplate/saramatestkit"
	"github.com/BaritoLog/go-boilerplate/slicekit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestKafkaTopics_NewClientError(t *testing.T) {
	patch := saramatestkit.PatchNewClient(nil, fmt.Errorf("some-error"))
	defer patch.Unpatch()

	_, err := kafkaTopics([]string{}, "_logs")
	FatalIfWrongError(t, err, "some-error")
}

func TestKafkaTopics_ErrorGetTopic(t *testing.T) {

	client := saramatestkit.NewClient()
	client.TopicsFunc = func() ([]string, error) {
		return nil, fmt.Errorf("some-error")
	}
	patch := saramatestkit.PatchNewClient(client, nil)
	defer patch.Unpatch()

	_, err := kafkaTopics([]string{}, "_logs")
	FatalIfWrongError(t, err, "some-error")
}

func TestKafkaTopics(t *testing.T) {
	want := []string{
		"asdf_logs",
		"qwer_logs",
		"lalaa",
	}
	client := saramatestkit.NewClient()
	client.TopicsFunc = func() ([]string, error) {
		return want, nil
	}
	patch := saramatestkit.PatchNewClient(client, nil)
	defer patch.Unpatch()

	got, err := kafkaTopics([]string{}, "_logs")
	FatalIfError(t, err)

	FatalIf(t, len(got) != 2, "len(got) must be 2")
	FatalIf(t, slicekit.StringSliceEqual(got, want), "got not equal want")
}

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
