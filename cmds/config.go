package cmds

import (
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	EnvKafkaBrokers       = "BARITO_KAFKA_BROKERS"
	EnvKafkaGroupID       = "BARITO_KAFKA_GROUP_ID"
	EnvKafkaTopicSuffix   = "BARITO_KAFKA_TOPIC_SUFFIX"
	EnvKafkaMaxRetry      = "BARITO_KAFKA_MAX_RETRY"
	EnvKafkaRetryInterval = "BARITO_KAFKA_RETRY_INTERVAL"

	EnvElasticsearchUrls = "BARITO_ELASTICSEARCH_URLS"
	EnvEsIndexMethod     = "BARITO_ELASTICSEARCH_INDEX_METHOD"
	EnvEsBulkSize        = "BARITO_ELASTICSEARCH_BULK_SIZE"
	EnvEsFlushIntervalMs = "BARITO_ELASTICSEARCH_FLUSH_INTERVAL_MS"

	EnvGrpcMaxRecvMsgSize = "BARITO_GRPC_MAX_RECV_MSG_SIZE"

	EnvPushMetricUrl      = "BARITO_PUSH_METRIC_URL"
	EnvPushMetricInterval = "BARITO_PUSH_METRIC_INTERVAL"

	EnvServeRestApi                   = "BARITO_PRODUCER_REST_API" // TODO: rename to better name
	EnvProducerAddressGrpc            = "BARITO_PRODUCER_GRPC"     // TODO: rename to better name
	EnvProducerAddressRest            = "BARITO_PRODUCER_REST"     // TODO: rename to better name
	EnvProducerMaxRetry               = "BARITO_PRODUCER_MAX_RETRY"
	EnvProducerMaxTPS                 = "BARITO_PRODUCER_MAX_TPS"
	EnvProducerRateLimitResetInterval = "BARITO_PRODUCER_RATE_LIMIT_RESET_INTERVAL"

	EnvConsulUrl               = "BARITO_CONSUL_URL"
	EnvConsulKafkaName         = "BARITO_CONSUL_KAFKA_NAME"
	EnvConsulElasticsearchName = "BARITO_CONSUL_ELASTICSEARCH_NAME"

	EnvNewTopicEventName                    = "BARITO_NEW_TOPIC_EVENT"
	EnvConsumerElasticsearchRetrierInterval = "BARITO_CONSUMER_ELASTICSEARCH_RETRIER_INTERVAL"
	EnvConsumerElasticsearchRetrierMaxRetry = "BARITO_CONSUMER_ELASTICSEARCH_RETRIER_MAX_RETRY"
	EnvConsumerRebalancingStrategy          = "BARITO_CONSUMER_REBALANCING_STRATEGY"

	EnvPrintTPS = "BARITO_PRINT_TPS"

	EnvElasticUsername = "ELASTIC_USERNAME"
	EnvElasticPassword = "ELASTIC_PASSWORD"
)

var (
	DefaultConsulKafkaName         = "kafka"
	DefaultConsulElasticsearchName = "elasticsearch"

	DefaultKafkaBrokers       = []string{"localhost:9092"}
	DefaultKafkaTopicSuffix   = "_logs"
	DefaultKafkaGroupID       = "barito-group"
	DefaultKafkaMaxRetry      = 0
	DefaultKafkaRetryInterval = 10

	DefaultElasticsearchUrls = []string{"http://localhost:9200"}

	DefaultGrpcMaxRecvMsgSize = 20 * 1000 * 1000

	DefaultPushMetricUrl      = ""
	DefaultPushMetricInterval = "30s"

	DefaultServeRestApi                   = "true"
	DefaultProducerAddressGrpc            = ":8082"
	DefaultProducerAddressRest            = ":8080"
	DefaultProducerMaxRetry               = 10
	DefaultProducerMaxTPS                 = 100
	DefaultProducerRateLimitResetInterval = 10

	DefaultNewTopicEventName            = "new_topic_events"
	DefaultElasticsearchRetrierInterval = "30s"
	DefaultElasticsearchRetrierMaxRetry = 10
	DefaultConsumerRebalancingStrategy  = "RoundRobin"
	DefaultEsIndexMethod                = "BulkProcessor"
	DefaultEsBulkSize                   = 100
	DefaultEsFlushIntervalMs            = 500

	DefaultPrintTPS = "false"

	DefaultElasticUsername = ""
	DefaultElasticPassword = ""
)

func configKafkaBrokers() (brokers []string) {
	brokers = sliceEnvOrDefault(EnvKafkaBrokers, ",", []string{})

	if len(brokers) != 0 {
		return
	}

	consulUrl := configConsulUrl()
	name := configConsulKafkaName()
	brokers, err := consulKafkaBroker(consulUrl, name)
	if err == nil {
		logConfig("consul", EnvKafkaBrokers, brokers)
		return
	}

	brokers = DefaultKafkaBrokers
	return
}

func configElasticsearchUrls() (urls []string) {
	urls = sliceEnvOrDefault(EnvElasticsearchUrls, ",", []string{})

	if len(urls) > 0 {
		return
	}

	consulUrl := configConsulUrl()
	name := configConsulElasticsearchName()
	urls, err := consulElasticsearchUrl(consulUrl, name)

	if err == nil {
		logConfig("consul", EnvElasticsearchUrls, urls)
		return
	}

	urls = DefaultElasticsearchUrls
	return
}

func configEsIndexMethod() (s string) {
	return stringEnvOrDefault(EnvEsIndexMethod, DefaultEsIndexMethod)
}

func configEsBulkSize() (i int) {
	return intEnvOrDefault(EnvEsBulkSize, DefaultEsBulkSize)
}

func configEsFlushIntervalMs() (i int) {
	return intEnvOrDefault(EnvEsFlushIntervalMs, DefaultEsFlushIntervalMs)
}

func configGrpcMaxRecvMsgSize() (i int) {
	return intEnvOrDefault(EnvGrpcMaxRecvMsgSize, DefaultGrpcMaxRecvMsgSize)
}

func configConsulElasticsearchName() (s string) {
	return stringEnvOrDefault(EnvConsulElasticsearchName, DefaultConsulElasticsearchName)
}

func configKafkaGroupId() (s string) {
	return stringEnvOrDefault(EnvKafkaGroupID, DefaultKafkaGroupID)
}

func configKafkaMaxRetry() (i int) {
	return intEnvOrDefault(EnvKafkaMaxRetry, DefaultKafkaMaxRetry)
}

func configKafkaRetryInterval() (i int) {
	return intEnvOrDefault(EnvKafkaRetryInterval, DefaultKafkaRetryInterval)
}

func configPushMetricUrl() (s string) {
	return stringEnvOrDefault(EnvPushMetricUrl, DefaultPushMetricUrl)
}

func configPushMetricInterval() (s string) {
	return stringEnvOrDefault(EnvPushMetricInterval, DefaultPushMetricInterval)
}

func configServeRestApi() bool {
	return (stringEnvOrDefault(EnvServeRestApi, DefaultServeRestApi) == "true")
}

func configProducerAddressGrpc() (s string) {
	return stringEnvOrDefault(EnvProducerAddressGrpc, DefaultProducerAddressGrpc)
}

func configProducerAddressRest() (s string) {
	return stringEnvOrDefault(EnvProducerAddressRest, DefaultProducerAddressRest)
}

func configProducerMaxRetry() (i int) {
	return intEnvOrDefault(EnvProducerMaxRetry, DefaultProducerMaxRetry)
}

func configProducerMaxTPS() (i int) {
	return intEnvOrDefault(EnvProducerMaxTPS, DefaultProducerMaxTPS)
}

func configProducerRateLimitResetInterval() (i int) {
	return intEnvOrDefault(EnvProducerRateLimitResetInterval, DefaultProducerRateLimitResetInterval)
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

func configElasticsearchRetrierInterval() string {
	return stringEnvOrDefault(EnvConsumerElasticsearchRetrierInterval, DefaultElasticsearchRetrierInterval)
}

func configElasticsearchRetrierMaxRetry() int {
	return intEnvOrDefault(EnvConsumerElasticsearchRetrierMaxRetry, DefaultElasticsearchRetrierMaxRetry)
}

func configConsumerRebalancingStrategy() string {
	return stringEnvOrDefault(EnvConsumerRebalancingStrategy, DefaultConsumerRebalancingStrategy)
}

func configPrintTPS() bool {
	return (stringEnvOrDefault(EnvPrintTPS, DefaultPrintTPS) == "true")
}

func configElasticUsername() (s string) {
	return stringEnvOrDefault(EnvElasticUsername, DefaultElasticUsername)
}

func configElasticPassword() (s string) {
	return stringEnvOrDefault(EnvElasticPassword, DefaultElasticPassword)
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
	log.WithField("config", source).Warnf("%s = %v", key, val)
}
