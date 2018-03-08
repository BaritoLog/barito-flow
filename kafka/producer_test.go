package kafka

import (
	"github.com/Shopify/sarama"
	saramaMock "github.com/Shopify/sarama/mocks"
	. "github.com/imantung/go-boilerplate/testkit"
	"testing"
)

func FakeSaramaConfig() sarama.Config {
	config := sarama.NewConfig()
	return *config
}

func MockSaramaSyncProducer(t *testing.T) sarama.SyncProducer {
	config := FakeSaramaConfig()
	return saramaMock.NewSyncProducer(t, &config)
}

func TestKafkaProducer_SendMessage(t *testing.T) {
	config := FakeSaramaConfig()
	syncProducer := saramaMock.NewSyncProducer(t, &config)
	syncProducer.ExpectSendMessageAndSucceed()

	_, _, err := syncProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "test-topic",
		Value: sarama.StringEncoder("SomeMessage"),
	})

	FatalIfError(t, err)
}

func TestServer_CreateSaramaSyncProducer(t *testing.T) {
	syncProducer := MockSaramaSyncProducer(t)
	FatalIf(t, syncProducer == nil, "Samara SyncProducer is nil")
}

func TestServer_Producer(t *testing.T) {
	producer, err := server.Producer()

	FatalIf(t, producer == nil, "producer is nil")
	FatalIfError(t, err)
}

func TestNewProducer(t *testing.T) {
	syncProducer := MockSaramaSyncProducer(t)
	MockProducerCreator := func() (producer sarama.SyncProducer, err error) {
		return syncProducer, nil
	}

	producer, _ := NewProducer(MockProducerCreator)

	FatalIf(t, producer.producer == nil, "Producer is nil")
}