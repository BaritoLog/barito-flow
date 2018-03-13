package common

import "github.com/Shopify/sarama"

type KafkaDownstreamConfig struct {
	Brokers          []string
	ProducerRetryMax int
}

func (c *KafkaDownstreamConfig) SaramaConfig() (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = c.ProducerRetryMax   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	return

}
