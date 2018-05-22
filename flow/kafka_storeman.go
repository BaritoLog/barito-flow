package flow

import (
	"github.com/BaritoLog/barito-flow/river"
	"github.com/Shopify/sarama"
)

type Storeman interface {
	Store(timber river.Timber) error
}

type kafkaStoreman struct {
	producer sarama.SyncProducer
	topic    string
}

func NewKafkaStoreman(producer sarama.SyncProducer, topic string) Storeman {
	return &kafkaStoreman{producer: producer, topic: topic}
}

func (k *kafkaStoreman) Store(timber river.Timber) (err error) {
	message := river.ConvertToKafkaMessage(timber, k.topic)
	_, _, err = k.producer.SendMessage(message)
	return
}
