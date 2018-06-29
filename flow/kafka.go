package flow

import (
	"github.com/Shopify/sarama"
)

func kafkaStore(producer sarama.SyncProducer, topic string, timber Timber) (err error) {
	message := ConvertToKafkaMessage(timber, topic)
	_, _, err = producer.SendMessage(message)
	return
}
