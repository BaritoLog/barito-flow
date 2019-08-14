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
	FatalIf(t, configConsulElasticsearchName() != DefaultConsulElasticsearchName, "should return default ")

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

func TestGetPushMetricInterval(t *testing.T) {
	FatalIf(t, configPushMetricInterval() != DefaultPushMetricInterval, "should return default ")

	os.Setenv(EnvPushMetricInterval, "22s")
	defer os.Clearenv()
	FatalIf(t, configPushMetricInterval() != "22s", "should get from env variable")
}

func TestGetProducerAddressGrpc(t *testing.T) {
	FatalIf(t, configProducerAddressGrpc() != DefaultProducerAddressGrpc, "should return default ")

	os.Setenv(EnvProducerAddressGrpc, ":12345")
	defer os.Clearenv()
	FatalIf(t, configProducerAddressGrpc() != ":12345", "should get from env variable")
}

func TestGetProducerAddressRest(t *testing.T) {
	FatalIf(t, configProducerAddressRest() != DefaultProducerAddressRest, "should return default ")

	os.Setenv(EnvProducerAddressRest, ":12345")
	defer os.Clearenv()
	FatalIf(t, configProducerAddressRest() != ":12345", "should get from env variable")
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

func TestConfigKafkaTopicSuffix(t *testing.T) {
	FatalIf(t, configKafkaTopicSuffix() != DefaultKafkaTopicSuffix, "should return default ")

	os.Setenv(EnvKafkaTopicSuffix, "some-topic-suffix")
	defer os.Clearenv()

	FatalIf(t, configKafkaTopicSuffix() != "some-topic-suffix", "should get from env variable")
}

func TestConfigNewTopicEventName(t *testing.T) {
	FatalIf(t, configNewTopicEvent() != DefaultNewTopicEventName, "should return default")

	os.Setenv(EnvNewTopicEventName, "some-new-topic-event")
	defer os.Clearenv()

	FatalIf(t, configNewTopicEvent() != "some-new-topic-event", "should get from env variable")
}

func TestGetConsumerElasticsearchRetrierInterval(t *testing.T) {
	FatalIf(t, configElasticsearchRetrierInterval() != DefaultElasticsearchRetrierInterval, "should return default ")

	os.Setenv(EnvConsumerElasticsearchRetrierInterval, "30s")
	defer os.Clearenv()
	FatalIf(t, configElasticsearchRetrierInterval() != "30s", "should get from env variable")
}
