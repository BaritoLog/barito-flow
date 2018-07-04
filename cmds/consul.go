package cmds

import (
	"fmt"

	"github.com/hashicorp/consul/api"
)

func consulKafkaBroker(address, name string) (brokers []string, err error) {
	client, err := consulClient(address)
	if err != nil {
		return
	}
	services, _, err := client.Catalog().Service(name, "", nil)
	if err != nil {
		return
	}

	for _, service := range services {
		brokers = append(brokers, fmt.Sprintf("%s:%d", service.ServiceAddress, service.ServicePort))
	}
	return
}

func consulElasticsearchUrl(address, name string) (url string, err error) {
	client, err := consulClient(address)
	if err != nil {
		return
	}

	services, _, err := client.Catalog().Service(name, "", nil)
	if err != nil {
		return
	}
	if len(services) < 1 {
		err = fmt.Errorf("No Service")
		return
	}

	srvAddress := services[0].ServiceAddress
	srvPort := services[0].ServicePort
	srvSchema, ok := services[0].ServiceMeta["http_schema"]
	if !ok {
		srvSchema = "http"
	}

	url = fmt.Sprintf("%s://%s:%d", srvSchema, srvAddress, srvPort)
	return
}

func consulClient(address string) (client *api.Client, err error) {
	if len(address) <= 0 {
		err = fmt.Errorf("No consul address")
		return
	}

	client, err = api.NewClient(&api.Config{
		Address: address,
	})
	return
}
