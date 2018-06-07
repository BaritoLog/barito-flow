package cmds

import (
	"fmt"
	"os"

	"github.com/BaritoLog/go-boilerplate/envkit"
	"github.com/hashicorp/consul/api"
)

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
