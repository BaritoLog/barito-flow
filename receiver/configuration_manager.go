package receiver

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
		kafkaBrokers: "ext-kafka.default.svc.cluster.local:9092",
	}
	return
}
