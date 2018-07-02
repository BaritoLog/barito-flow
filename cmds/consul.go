package cmds

import (
	"fmt"
	"os"

	"github.com/BaritoLog/go-boilerplate/envkit"
	"github.com/hashicorp/consul/api"
)

func consulKafkaBroker() (brokers []string, err error) {
	name := envkit.GetString(EnvConsulKafkaName, DefaultConsulKafkaName)
	services, err := catalogServices(name)
	if err != nil {
		return
	}

	for _, service := range services {
		brokers = append(brokers, fmt.Sprintf("%s:%d", service.ServiceAddress, service.ServicePort))
	}
	return
}

func consulElasticsearchUrl() (url string, err error) {
	name := getConsulElasticsearchName()
	services, err := catalogServices(name)
	if err != nil {
		return
	}
	if len(services) < 1 {
		err = fmt.Errorf("No Service")
		return
	}

	address := services[0].ServiceAddress
	port := services[0].ServicePort
	schema, ok := services[0].ServiceMeta["http_schema"]
	if !ok {
		schema = "http"
	}

	url = fmt.Sprintf("%s://%s:%d", schema, address, port)
	return
}

// TODO: make consulClient as function parameter
func catalogServices(name string) (services []*api.CatalogService, err error) {
	consulAddr, ok := os.LookupEnv(EnvConsulUrl)
	if !ok {
		err = fmt.Errorf("no ENV %s", EnvConsulUrl)
		return
	}
	consulClient, _ := api.NewClient(&api.Config{Address: consulAddr})
	services, _, err = consulClient.Catalog().Service(name, "", nil)
	return
}
