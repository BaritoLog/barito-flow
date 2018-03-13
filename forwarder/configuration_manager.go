package forwarder

import "github.com/BaritoLog/go-boilerplate/app"

// ConfigurationManager
type ConfigurationManager interface {
	Retrieve() (config app.Configuration, err error)
}

type configurationManager struct {
}

func NewConfigurationManager() ConfigurationManager {
	return &configurationManager{}
}

func (m configurationManager) Retrieve() (config app.Configuration, err error) {
	// TODO: get configuration from server
	config = Configuration{
		addr: ":8080",
		kafkaBrokers: "localhost:9092",
		kafkaConsumerGroupId: "barito-consumer",
		kafkaConsumerTopic: "kafka-dummy-topic",
		elasticsearchUrls: "http://localhost:9200",
		elasticsearchIndexPrefix: "baritolog",
	}
	return
}
