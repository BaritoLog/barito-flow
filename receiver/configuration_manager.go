package receiver

import "barito-agent/common/app"

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
	}
	return
}
