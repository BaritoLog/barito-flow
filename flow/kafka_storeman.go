package flow

import (
	"github.com/Shopify/sarama"
)

type Storeman interface {
	Store(timber Timber) error
}

type kafkaStoreman struct {
	producer sarama.SyncProducer
	topic    string
}

func NewKafkaStoreman(producer sarama.SyncProducer, topic string) Storeman {
	return &kafkaStoreman{producer: producer, topic: topic}
}

func (k *kafkaStoreman) Store(timber Timber) (err error) {
	message := ConvertToKafkaMessage(timber, k.topic)
	_, _, err = k.producer.SendMessage(message)
	return
}
