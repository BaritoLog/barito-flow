package cmds

import (
	"os"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestGetKafkaBrokers_NoConsulAndNoEnv(t *testing.T) {
	brokers := getKafkaBrokers()

	FatalIf(t, len(brokers) != len(DefaultKafkaBrokers), "wrong brokers")
	for i, _ := range brokers {
		FatalIf(t, brokers[i] != DefaultKafkaBrokers[i], "wrong broker item")
	}
}

func TestGetKafkaBrokers_FromEnv(t *testing.T) {
	os.Setenv(EnvKafkaBrokers, "kafka-broker-1:1278,kafka-broker-2:1288")
	defer os.Clearenv()

	brokers := getKafkaBrokers()
	FatalIf(t, len(brokers) != 2, "wrong brokers")
	FatalIf(t, brokers[0] != "kafka-broker-1:1278", "wrong brokers[0]")
	FatalIf(t, brokers[1] != "kafka-broker-2:1288", "wrong brokers[1]")
}

func TestGetConsulElastisearchName(t *testing.T) {
	FatalIf(t, getConsulElasticsearchName() != DefaultElasticsearchName, "should return default ")

	os.Setenv(EnvConsulElasticsearchName, "elastic11")
	defer os.Clearenv()

	FatalIf(t, getConsulElasticsearchName() != "elastic11", "should get from env variable")
}

func TestGetElasticsearchUrl(t *testing.T) {

	FatalIf(t, getElasticsearchUrl() != DefaultElasticsearchUrl, "should return default ")

	os.Setenv(EnvElasticsearchUrl, "http://some-elasticsearch")
	defer os.Clearenv()

	FatalIf(t, getElasticsearchUrl() != "http://some-elasticsearch", "should get from env variable")

}
