package cmds

import (
	"os"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

// func TestGetKafkaBrokers(t *testing.T) {
// 	FatalIf(t, !slicekit.StringSliceEqual(getKafkaBrokers(), DefaultKafkaBrokers), "should return default ")
//
// 	os.Setenv(EnvKafkaBrokers, "kafka-broker-1:1278,kafka-broker-2:1288")
// 	defer os.Clearenv()
//
// 	FatalIf(t, !slicekit.StringSliceEqual(getKafkaBrokers(), []string{"kafka-broker-1:1278", "kafka-broker-2:1288"}), "should return default ")
// }

func TestGetConsulElastisearchName(t *testing.T) {
	FatalIf(t, configConsulElasticsearchName() != DefaultElasticsearchName, "should return default ")

	os.Setenv(EnvConsulElasticsearchName, "elastic11")
	defer os.Clearenv()
	FatalIf(t, configConsulElasticsearchName() != "elastic11", "should get from env variable")
}

func TestGetElasticsearchUrl(t *testing.T) {
	FatalIf(t, configElasticsearchUrl() != DefaultElasticsearchUrl, "should return default ")

	os.Setenv(EnvElasticsearchUrl, "http://some-elasticsearch")
	defer os.Clearenv()
	FatalIf(t, configElasticsearchUrl() != "http://some-elasticsearch", "should get from env variable")
}

func TestGetKafkaGroupID(t *testing.T) {
	FatalIf(t, configKafkaGroupId() != DefaultKafkaGroupID, "should return default ")

	os.Setenv(EnvKafkaGroupID, "some-group-id")
	defer os.Clearenv()
	FatalIf(t, configKafkaGroupId() != "some-group-id", "should get from env variable")
}

func TestGetPushMetricUrl(t *testing.T) {
	FatalIf(t, configPushMetricUrl() != DefaultPushMetricUrl, "should return default ")

	os.Setenv(EnvPushMetricUrl, "http://some-push-metric")
	defer os.Clearenv()
	FatalIf(t, configPushMetricUrl() != "http://some-push-metric", "should get from env variable")
}

func TestGetPushMetricToken(t *testing.T) {
	FatalIf(t, configPushMetricToken() != DefaultPushMetricToken, "should return default ")

	os.Setenv(EnvPushMetricToken, "some-token")
	defer os.Clearenv()
	FatalIf(t, configPushMetricToken() != "some-token", "should get from env variable")
}

func TestGetPushMetricInterval(t *testing.T) {
	FatalIf(t, configPushMetricInterval() != DefaultPushMetricInterval, "should return default ")

	os.Setenv(EnvPushMetricInterval, "22s")
	defer os.Clearenv()
	FatalIf(t, configPushMetricInterval() != "22s", "should get from env variable")
}

func TestGetProducerAddress(t *testing.T) {
	FatalIf(t, configProducerAddress() != DefaultProducerAddress, "should return default ")

	os.Setenv(EnvProducerAddress, ":12345")
	defer os.Clearenv()
	FatalIf(t, configProducerAddress() != ":12345", "should get from env variable")
}

func TestGetProducerMaxRetry(t *testing.T) {
	FatalIf(t, configProducerMaxRetry() != DefaultProducerMaxRetry, "should return default ")

	os.Setenv(EnvProducerMaxRetry, "989")
	defer os.Clearenv()
	FatalIf(t, configProducerMaxRetry() != 989, "should get from env variable")
}

func TestGetProducerMaxTPS(t *testing.T) {
	FatalIf(t, configProducerMaxTPS() != DefaultProducerMaxTPS, "should return default ")

	os.Setenv(EnvProducerMaxTPS, "222")
	defer os.Clearenv()
	FatalIf(t, configProducerMaxTPS() != 222, "should get from env variable")
}

func TestConfigConsulKafkaName(t *testing.T) {
	FatalIf(t, configConsulKafkaName() != DefaultConsulKafkaName, "should return default ")

	os.Setenv(EnvConsulKafkaName, "some-kafka-name")
	defer os.Clearenv()

	FatalIf(t, configConsulKafkaName() != "some-kafka-name", "should get from env variable")

}
