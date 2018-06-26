package cmds

import (
	"os"
	"testing"

	"github.com/BaritoLog/go-boilerplate/slicekit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestGetKafkaBrokers(t *testing.T) {
	FatalIf(t, !slicekit.StringSliceEqual(getKafkaBrokers(), DefaultKafkaBrokers), "should return default ")

	os.Setenv(EnvKafkaBrokers, "kafka-broker-1:1278,kafka-broker-2:1288")
	defer os.Clearenv()

	FatalIf(t, !slicekit.StringSliceEqual(getKafkaBrokers(), []string{"kafka-broker-1:1278", "kafka-broker-2:1288"}), "should return default ")
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

func TestGetKafkaConsumerTopics(t *testing.T) {
	FatalIf(t, !slicekit.StringSliceEqual(getKafkaConsumerTopics(), DefaultKafkaConsumerTopics), "should return default ")

	os.Setenv(EnvKafkaConsumerTopics, "some-topic-01,some-topic-02")
	defer os.Clearenv()
	FatalIf(t, !slicekit.StringSliceEqual(getKafkaConsumerTopics(), []string{"some-topic-01", "some-topic-02"}), "should get from env variable")
}

func TestGetKafkaGroupID(t *testing.T) {
	FatalIf(t, getKafkaGroupId() != DefaultKafkaGroupID, "should return default ")

	os.Setenv(EnvKafkaGroupID, "some-group-id")
	defer os.Clearenv()
	FatalIf(t, getKafkaGroupId() != "some-group-id", "should get from env variable")
}

func TestGetPushMetricUrl(t *testing.T) {
	FatalIf(t, getPushMetricUrl() != DefaultPushMetricUrl, "should return default ")

	os.Setenv(EnvPushMetricUrl, "http://some-push-metric")
	defer os.Clearenv()
	FatalIf(t, getPushMetricUrl() != "http://some-push-metric", "should get from env variable")
}

func TestGetPushMetricToken(t *testing.T) {
	FatalIf(t, getPushMetricToken() != DefaultPushMetricToken, "should return default ")

	os.Setenv(EnvPushMetricToken, "some-token")
	defer os.Clearenv()
	FatalIf(t, getPushMetricToken() != "some-token", "should get from env variable")
}

func TestGetPushMetricInterval(t *testing.T) {
	FatalIf(t, getPushMetricInterval() != DefaultPushMetricInterval, "should return default ")

	os.Setenv(EnvPushMetricInterval, "22s")
	defer os.Clearenv()
	FatalIf(t, getPushMetricInterval() != "22s", "should get from env variable")
}

func TestGetProducerAddress(t *testing.T) {
	FatalIf(t, getProducerAddress() != DefaultProducerAddress, "should return default ")

	os.Setenv(EnvProducerAddress, ":12345")
	defer os.Clearenv()
	FatalIf(t, getProducerAddress() != ":12345", "should get from env variable")
}

func TestGetProducerMaxRetry(t *testing.T) {
	FatalIf(t, getProducerMaxRetry() != DefaultProducerMaxRetry, "should return default ")

	os.Setenv(EnvProducerMaxRetry, "989")
	defer os.Clearenv()
	FatalIf(t, getProducerMaxRetry() != 989, "should get from env variable")
}

func TestGetProducerMaxTPS(t *testing.T) {
	FatalIf(t, getProducerMaxTPS() != DefaultProducerMaxTPS, "should return default ")

	os.Setenv(EnvProducerMaxTPS, "222")
	defer os.Clearenv()
	FatalIf(t, getProducerMaxTPS() != 222, "should get from env variable")
}

func TestGetKafkaProducerTopic(t *testing.T) {
	FatalIf(t, getKafkaProducerTopic() != DefaultKafkaProducerTopic, "should return default ")

	os.Setenv(EnvKafkaProducerTopic, "some-topic")
	defer os.Clearenv()
	FatalIf(t, getKafkaProducerTopic() != "some-topic", "should get from env variable")

}
