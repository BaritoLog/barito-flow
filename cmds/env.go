package cmds

import (
	"github.com/BaritoLog/go-boilerplate/envkit"
)

// TODO: rename env.go to config.go
// TODO: rename function name using config prefix
// TODO: using log.info() get config value and config source

const (
	EnvKafkaBrokers        = "BARITO_KAFKA_BROKERS"
	EnvKafkaGroupID        = "BARITO_KAFKA_GROUP_ID"
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

	DefaultElasticsearchUrl    = "http://localhost:9200"
	DefaultKafkaConsumerTopics = []string{"consumer-topic"}
	DefaultKafkaGroupID        = "barito-group"
	DefaultPushMetricUrl       = "http://localhost:3000/api/increase_log_count"
	DefaultPushMetricToken     = ""
	DefaultPushMetricInterval  = "30s"
	DefaultProducerAddress     = ":8080"
	DefaultProducerMaxRetry    = 10
	DefaultKafkaProducerTopic  = "producer-topic"
	DefaultProducerMaxTPS      = 100
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

func getKafkaConsumerTopics() []string {
	return envkit.GetSlice(EnvKafkaConsumerTopics, ",", DefaultKafkaConsumerTopics)
}

func getKafkaGroupId() string {
	return envkit.GetString(EnvKafkaGroupID, DefaultKafkaGroupID)
}

func getPushMetricUrl() string {
	return envkit.GetString(EnvPushMetricUrl, DefaultPushMetricUrl)
}

func getPushMetricToken() string {
	return envkit.GetString(EnvPushMetricToken, DefaultPushMetricToken)
}

func getPushMetricInterval() string {
	return envkit.GetString(EnvPushMetricInterval, DefaultPushMetricInterval)
}

func getProducerAddress() string {
	return envkit.GetString(EnvProducerAddress, DefaultProducerAddress)
}

func getProducerMaxRetry() int {
	return envkit.GetInt(EnvProducerMaxRetry, DefaultProducerMaxRetry)
}

func getKafkaProducerTopic() string {
	return envkit.GetString(EnvKafkaProducerTopic, DefaultKafkaProducerTopic)
}

func getProducerMaxTPS() int {
	return envkit.GetInt(EnvProducerMaxTPS, DefaultProducerMaxTPS)
}
