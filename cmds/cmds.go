package cmds

import (
	"fmt"
	"os"

	"github.com/BaritoLog/go-boilerplate/envkit"
	"github.com/hashicorp/consul/api"
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

	EnvConsulUrl       = "BARITO_CONSUL_URL"
	EnvConsulKafkaName = "BARITO_CONSUL_KAFKA_NAME"
)

var (
	DefaultConsulKafkaName = "kafka"
	DefaultKafkaBrokers    = []string{"localhost:9092"}
)

// return kafka broker addresses
func GetKafkaBrokers() (brokers []string) {
	brokers, err := getKafkaBrokerFromConsul()
	if err != nil {
		brokers = envkit.GetSlice(EnvKafkaBrokers, ",", DefaultKafkaBrokers)
	}
	return
}

func getKafkaBrokerFromConsul() (brokers []string, err error) {
	consulAddr, ok := os.LookupEnv(EnvConsulUrl)
	if !ok {
		err = fmt.Errorf("no ENV %s", EnvConsulUrl)
		return
	}

	name := envkit.GetString(EnvConsulKafkaName, DefaultConsulKafkaName)
	consulClient, _ := api.NewClient(&api.Config{
		Address: consulAddr,
	})
	services, _, err := consulClient.Catalog().Service(name, "", nil)
	if err != nil {
		return
	}
	for _, service := range services {
		brokers = append(brokers, fmt.Sprintf("%s:%d", service.ServiceAddress, service.ServicePort))
	}
	return
}
