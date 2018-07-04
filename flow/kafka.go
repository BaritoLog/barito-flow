package flow

import (
	"strings"

	"github.com/Shopify/sarama"
)

func kafkaTopics(brokers []string, suffix string) (topics []string, err error) {

	config := sarama.NewConfig()
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return
	}
	defer client.Close()

	topics0, err := client.Topics()
	if err != nil {
		return
	}

	for _, topic := range topics0 {
		if strings.HasSuffix(topic, suffix) {
			topics = append(topics, topic)
		}

	}
	return
}

func kafkaStore(producer sarama.SyncProducer, timber Timber, suffix string) (err error) {
	topic := timber.Context().KafkaTopic
	message := ConvertTimberToKafkaMessage(timber, topic+suffix)
	_, _, err = producer.SendMessage(message)
	return
}
