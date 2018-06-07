package cmds

import (
	"net/http"
	"os"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestGetKafkaBrokers_NoConsulAndNoEnv(t *testing.T) {
	brokers := GetKafkaBrokers()

	FatalIf(t, len(brokers) != len(DefaultKafkaBrokers), "wrong brokers")
	for i, _ := range brokers {
		FatalIf(t, brokers[i] != DefaultKafkaBrokers[i], "wrong broker item")
	}
}

func TestGetKafkaBorkersFromConsul_NoEnv(t *testing.T) {
	_, err := getKafkaBrokerFromConsul()
	FatalIfWrongError(t, err, "no ENV BARITO_CONSUL_URL")
}

func TestGetKafkaBorkersFromConsul_WrongConsulAddress(t *testing.T) {
	os.Setenv(EnvConsulUrl, "http://wrong-consul")
	_, err := getKafkaBrokerFromConsul()
	FatalIfWrongError(t, err, "Get http://wrong-consul/v1/catalog/service/kafka: dial tcp: lookup wrong-consul: no such host")
}

func TestGetKafkaBorkersFromConsul(t *testing.T) {
	ts := NewHttpTestServer(http.StatusOK, []byte(`[
  {
    "ServiceAddress": "172.17.0.3",
    "ServicePort": 5000
  },
  {
    "ServiceAddress": "172.17.0.4",
    "ServicePort": 5001
  }
]`))

	os.Setenv(EnvConsulUrl, ts.URL)

	brokers, err := getKafkaBrokerFromConsul()
	FatalIfError(t, err)
	FatalIf(t, len(brokers) != 2, "return wrong brokers")
	FatalIf(t, brokers[0] != "172.17.0.3:5000", "return wrong brokers[0]")
	FatalIf(t, brokers[1] != "172.17.0.4:5001", "return wrong brokers[1]")

}
