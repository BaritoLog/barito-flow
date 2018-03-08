package kafka

import (
	"github.com/Shopify/sarama"
	"strings"
)

// Producer interface for Kafka
type Producer interface {
	SendMessage(string, string) error
}

// Producer struct with sarama SyncProducer
type KafkaProducer struct {
	producer sarama.SyncProducer
}

// Function type for Producer
type ProducerCreator func() (producer sarama.SyncProducer, err error)

// Implementation of Producer interface, return Sarama SyncProducer
func (s Server) Producer() (producer Producer, err error) {
	return NewProducer(s.CreateSaramaSyncProducer)
}

// Take a ProducerCreator function type and return KafkaProducer
func NewProducer(creator ProducerCreator) (kafkaProducer KafkaProducer, err error) {
	syncProducer, err := creator()
	return KafkaProducer{
		producer: syncProducer,
	}, err
}

// Initiate sarama SyncProducer with configs
func (s Server) CreateSaramaSyncProducer() (producer sarama.SyncProducer, err error) {
	brokerList := strings.Split(s.brokers, ",")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	return sarama.NewSyncProducer(brokerList, config)
}

// Implementation of SendMessage
func (k KafkaProducer) SendMessage(topic string, message string) error {
	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	})

	return err
}
