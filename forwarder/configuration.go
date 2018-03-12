package forwarder

type Configuration struct {
	addr string
	kafkaBrokers string
	kafkaConsumerGroupId string
	kafkaConsumerTopic string
}
