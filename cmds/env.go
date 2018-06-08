package cmds

import (
	"github.com/BaritoLog/go-boilerplate/envkit"
)

const (
	EnvKafkaBrokers        = "BARITO_KAFKA_BROKERS"
	EnvKafkaGroupId        = "BARITO_KAFKA_GROUP_ID"
	EnvKafkaConsumerTopics = "BARITO_KAFKA_CONSUMER_TOPICS"
	EnvKafkaProducerTopic  = "BARITO_KAFKA_PRODUCER_TOPIC"
	EnvElasticsearchUrl    = "BARITO_ELASTICSEARCH_URL"

	EnvPushMetricUrl      = "BARITO_PUSH_METRIC_URL"
	EnvPushMetricToken    = "BARITO_PUSH_METRIC_TOKEN"
	EnvPushMetricInterval = "BARITO_PUSH_METRIC_INTERVAL"

	EnvProducerAddress  = "BARITO_PRODUCER_ADDRESS"
	EnvProducerMaxRetry = "BARITO_PRODUCER_MAX_RETRY"
	EnvProducerMaxTPS   = "BARITO_PRODUCER_MAX_TPS"

	EnvConsulUrl               = "BARITO_CONSUL_URL"
	EnvConsulKafkaName         = "BARITO_CONSUL_KAFKA_NAME"
	EnvConsulElasticsearchName = "BARITO_CONSUL_ELASTICSEARCH_NAME"
)

var (
	DefaultConsulKafkaName   = "kafka"
	DefaultElasticsearchName = "elasticsearch"
	DefaultKafkaBrokers      = []string{"localhost:9092"}

	DefaultElasticsearchUrl = "http://localhost:9200"
)

func getKafkaBrokers() (brokers []string) {
	brokers, err := consulKafkaBroker()
	if err != nil {
		brokers = envkit.GetSlice(EnvKafkaBrokers, ",", DefaultKafkaBrokers)
	}
	return
}

func getElasticsearchUrl() (url string) {
	url, err := consulElasticsearchUrl()
	if err != nil {
		url = envkit.GetString(EnvElasticsearchUrl, DefaultElasticsearchUrl)
	}

	return
}

func getConsulElasticsearchName() string {
	return envkit.GetString(EnvConsulElasticsearchName, DefaultElasticsearchName)
}
