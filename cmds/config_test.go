package cmds

import (
	"os"
	"testing"

	"github.com/BaritoLog/go-boilerplate/slicekit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

func TestGetKafkaBrokers(t *testing.T) {
	FatalIf(t, !slicekit.StringSliceEqual(configKafkaBrokers(), DefaultKafkaBrokers), "should return default")

	os.Setenv(EnvKafkaBrokers, "some-kafka-01:9092, some-kafka-02:9092")
	defer os.Clearenv()

	envKafkaBrokers := sliceEnvOrDefault(EnvKafkaBrokers, ",", DefaultKafkaBrokers)
	FatalIf(t, !slicekit.StringSliceEqual(configKafkaBrokers(), envKafkaBrokers), "should get from env variable")
}

func TestGetConsulElastisearchName(t *testing.T) {
	FatalIf(t, configConsulElasticsearchName() != DefaultConsulElasticsearchName, "should return default ")

	os.Setenv(EnvConsulElasticsearchName, "elastic11")
	defer os.Clearenv()
	FatalIf(t, configConsulElasticsearchName() != "elastic11", "should get from env variable")
}

func TestGetElasticsearchUrls(t *testing.T) {
	FatalIf(t, !slicekit.StringSliceEqual(configElasticsearchUrls(), DefaultElasticsearchUrls), "should return default")

	os.Setenv(EnvElasticsearchUrls, "http://some-elasticsearch-01:9200, http://some-elasticsearch-02:9200")
	defer os.Clearenv()

	envEsUrls := sliceEnvOrDefault(EnvElasticsearchUrls, ",", DefaultElasticsearchUrls)
	FatalIf(t, !slicekit.StringSliceEqual(configElasticsearchUrls(), envEsUrls), "should get from env variable")
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
