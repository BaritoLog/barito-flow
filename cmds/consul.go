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

func consulElasticsearchUrl(address, name string) (urls []string, err error) {
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

	for _, service := range services {
		srvAddress := service.ServiceAddress
		srvPort := service.ServicePort
		srvSchema, ok := service.ServiceMeta["http_schema"]
		if !ok {
			srvSchema = "http"
		}
		urls = append(urls, fmt.Sprintf("%s://%s:%d", srvSchema, srvAddress, srvPort))
	}
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
