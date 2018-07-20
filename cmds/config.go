package cmds

import (
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	EnvKafkaBrokers     = "BARITO_KAFKA_BROKERS"
	EnvKafkaGroupID     = "BARITO_KAFKA_GROUP_ID"
	EnvKafkaTopicSuffix = "BARITO_KAFKA_TOPIC_SUFFIX"
	EnvElasticsearchUrl = "BARITO_ELASTICSEARCH_URL"

	EnvPushMetricUrl      = "BARITO_PUSH_METRIC_URL"
	EnvPushMetricToken    = "BARITO_PUSH_METRIC_TOKEN"
	EnvPushMetricInterval = "BARITO_PUSH_METRIC_INTERVAL"

	EnvProducerAddress  = "BARITO_PRODUCER_ADDRESS" // TODO: rename to better name
	EnvProducerMaxRetry = "BARITO_PRODUCER_MAX_RETRY"
	EnvProducerMaxTPS   = "BARITO_PRODUCER_MAX_TPS"

	EnvConsulUrl               = "BARITO_CONSUL_URL"
	EnvConsulKafkaName         = "BARITO_CONSUL_KAFKA_NAME"
	EnvConsulElasticsearchName = "BARITO_CONSUL_ELASTICSEARCH_NAME"

	EnvNewTopicEventName = "BARITO_NEW_TOPIC_EVENT"
)

var (
	DefaultConsulKafkaName         = "kafka"
	DefaultConsulElasticsearchName = "elasticsearch"

	DefaultKafkaBrokers     = []string{"localhost:9092"}
	DefaultKafkaTopicSuffix = "_logs"
	DefaultKafkaGroupID     = "barito-group"

	DefaultElasticsearchUrl = "http://localhost:9200"

	DefaultPushMetricUrl      = "http://localhost:3000/api/increase_log_count"
	DefaultPushMetricToken    = ""
	DefaultPushMetricInterval = "30s"

	DefaultProducerAddress  = ":8080"
	DefaultProducerMaxRetry = 10
	DefaultProducerMaxTPS   = 100

	DefaultNewTopicEventName = "new_topic_events"
)

func configKafkaBrokers() (brokers []string) {
	consulUrl := configConsulUrl()
	name := configConsulKafkaName()
	brokers, err := consulKafkaBroker(consulUrl, name)
	if err != nil {
		brokers = sliceEnvOrDefault(EnvKafkaBrokers, ",", DefaultKafkaBrokers)
		return
	}

	logConfig("consul", EnvKafkaBrokers, brokers)
	return
}

func configElasticsearchUrl() (url string) {
	consulUrl := configConsulUrl()
	name := configConsulElasticsearchName()
	url, err := consulElasticsearchUrl(consulUrl, name)
	if err != nil {
		url = stringEnvOrDefault(EnvElasticsearchUrl, DefaultElasticsearchUrl)
		return
	}

	logConfig("consul", EnvElasticsearchUrl, url)
	return
}

func configConsulElasticsearchName() (s string) {
	return stringEnvOrDefault(EnvConsulElasticsearchName, DefaultConsulElasticsearchName)
}

func configKafkaGroupId() (s string) {
	return stringEnvOrDefault(EnvKafkaGroupID, DefaultKafkaGroupID)
}

func configPushMetricUrl() (s string) {
	return stringEnvOrDefault(EnvPushMetricUrl, DefaultPushMetricUrl)
}

func configPushMetricToken() (s string) {
	return stringEnvOrDefault(EnvPushMetricToken, DefaultPushMetricToken)
}

func configPushMetricInterval() (s string) {
	return stringEnvOrDefault(EnvPushMetricInterval, DefaultPushMetricInterval)
}

func configProducerAddress() (s string) {
	return stringEnvOrDefault(EnvProducerAddress, DefaultProducerAddress)
}

func configProducerMaxRetry() (i int) {
	return intEnvOrDefault(EnvProducerMaxRetry, DefaultProducerMaxRetry)
}

func configProducerMaxTPS() (i int) {
	return intEnvOrDefault(EnvProducerMaxTPS, DefaultProducerMaxTPS)
}

func configConsulKafkaName() (s string) {
	return stringEnvOrDefault(EnvConsulKafkaName, DefaultConsulKafkaName)
}

func configConsulUrl() (s string) {
	return os.Getenv(EnvConsulUrl)
}

func configKafkaTopicSuffix() string {
	return stringEnvOrDefault(EnvKafkaTopicSuffix, DefaultKafkaTopicSuffix)
}

func configNewTopicEvent() string {
	return stringEnvOrDefault(EnvNewTopicEventName, DefaultNewTopicEventName)

}

func stringEnvOrDefault(key, defaultValue string) string {
	s := os.Getenv(key)
	if len(s) > 0 {
		logConfig("env", key, s)
		return s
	}

	logConfig("default", key, defaultValue)
	return defaultValue
}

func intEnvOrDefault(key string, defaultValue int) int {
	s := os.Getenv(key)
	i, err := strconv.Atoi(s)
	if err == nil {
		logConfig("env", key, i)
		return i
	}

	logConfig("default", key, defaultValue)
	return defaultValue
}

func sliceEnvOrDefault(key, separator string, defaultSlice []string) []string {
	s := os.Getenv(key)

	if len(s) > 0 {
		slice := strings.Split(s, separator)
		logConfig("env", key, slice)
		return slice
	}

	logConfig("default", key, defaultSlice)
	return defaultSlice
}

func logConfig(source, key string, val interface{}) {
	log.WithField("config", source).Infof("%s = %v", key, val)
}
