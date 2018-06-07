package cmds

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

	EnvConsulUrl       = "BARITO_CONSUL_URL"
	EnvConsulKafkaName = "BARITO_CONSUL_KAFKA_NAME"
)

var (
	DefaultConsulKafkaName = "kafka"
	DefaultKafkaBrokers    = []string{"localhost:9092"}
)
