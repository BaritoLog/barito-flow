package flow

import (
	"github.com/Shopify/sarama"
)

func kafkaStore(producer sarama.SyncProducer, timber Timber, suffix string) (err error) {
	topic := timber.Context().KafkaTopic
	message := ConvertTimberToKafkaMessage(timber, topic+suffix)
	_, _, err = producer.SendMessage(message)
	return
}
