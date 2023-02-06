package prome

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	pb "github.com/vwidjaya/barito-proto/producer"
)

const (
	ESClientFailedPhaseInit  = "init"
	ESClientFailedPhaseRetry = "retry"
)

var consumerTimberConvertError *prometheus.CounterVec
var consumerLogStoredCounter *prometheus.CounterVec
var consumerBulkProcessTimeSecond prometheus.Summary
var consumerKafkaMessagesIncomingCounter *prometheus.CounterVec
var consumerElasticsearchClientFailed *prometheus.CounterVec
var producerKafkaMessageStoredTotal *prometheus.CounterVec
var producerTPSExceededCounter *prometheus.CounterVec
var producerSendToKafkaTimeSecond *prometheus.SummaryVec
var producerKafkaClientFailed *prometheus.CounterVec
var producerTotalLogBytesIngested *prometheus.CounterVec

var indexDatePattern *regexp.Regexp = regexp.MustCompile(`-\d{4}\.\d{2}\.\d{2}$`)

var logStoredErrorMap map[string]string = map[string]string{
	"the final mapping":           "multiple_type",
	"to parse field":              "mapping_failed",
	"mapping":                     "mapping_failed",
	"mapper":                      "mapping_failed",
	"Data too large":              "data_too_large",
	"TransportService is closed":  "request_error_transport_service_closed",
	"Node not connected":          "node_not_connected",
	"primary shard is not active": "inactive_primary_shard",
	"no such shard":               "no_such_shard",
	"index read-only":             "index_read_only",
	"Limit of total fields":       "limit_of_total_fields_excedeed",
}

func InitConsumerInstrumentation() {
	consumerTimberConvertError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barito_consumer_timber_convert_error",
		Help: "Number error when read timber",
	}, []string{"index"})
	consumerLogStoredCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barito_consumer_log_stored_total",
		Help: "Number log stored to ES",
	}, []string{"index", "status", "result", "error"})
	consumerBulkProcessTimeSecond = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "barito_consumer_bulk_process_time_second",
		Help:       "Bulk process time in second ",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	consumerKafkaMessagesIncomingCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barito_consumer_kafka_message_incoming_total",
		Help: "Number of messages incoming from kafka",
	}, []string{"topic"})
	consumerElasticsearchClientFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barito_consumer_elasticsearch_client_failed",
		Help: "Number of elasticsearch client failed",
	}, []string{"phase"})
}

func InitProducerInstrumentation() {
	producerKafkaMessageStoredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barito_producer_kafka_message_stored_total",
		Help: "Number of message stored to kafka",
	}, []string{"topic", "error_type"})
	producerTPSExceededCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barito_producer_tps_exceeded_total",
		Help: "Number of TPS exceeded event",
	}, []string{"topic"})
	producerKafkaClientFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barito_producer_kafka_client_failed",
		Help: "Number of client failed to connect to kafka",
	}, []string{})
	producerSendToKafkaTimeSecond = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "barito_producer_send_to_kafka_time_second",
		Help:       "Send to Kafka time in second",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"topic"})
	producerTotalLogBytesIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barito_producer_produced_total_log_bytes",
		Help: "Total log bytes being ingested by the producer",
	}, []string{"app_name"})
}

func IncreaseConsumerTimberConvertError(index string) {
	consumerTimberConvertError.WithLabelValues(index).Inc()
}

func ObserveByteIngestion(topic string, suffix string, timber *pb.Timber) {
	re := regexp.MustCompile(suffix + "$")
	appName := re.ReplaceAllString(topic, "")
	b, _ := proto.Marshal(timber)
	producerTotalLogBytesIngested.WithLabelValues(appName).Add(float64(len(b)))
}

func IncreaseLogStoredCounter(index string, result string, status int, errorMessage string) {
	errorType := ""
	if errorMessage != "" {
		errorType = "undefined_error"
		for k, v := range logStoredErrorMap {
			if strings.Contains(errorMessage, k) {
				errorType = v
				break
			}
		}

		if errorType == "undefined_error" {
			log.Errorf("Found undefined error when consumer fail: %s", errorMessage)
		}
	}

	indexWithoutDate := indexDatePattern.ReplaceAllString(index, "")
	consumerLogStoredCounter.WithLabelValues(indexWithoutDate, result, strconv.Itoa(status), errorType).Inc()
}

func IncreaseKafkaMessagesIncoming(topic string) {
	consumerKafkaMessagesIncomingCounter.WithLabelValues(topic).Inc()
}

func ObserveBulkProcessTime(elapsedTime float64) {
	consumerBulkProcessTimeSecond.Observe(elapsedTime)
}

func IncreaseConsumerElasticsearchClientFailed(phase string) {
	consumerElasticsearchClientFailed.WithLabelValues(phase).Inc()
}

func IncreaseKafkaMessagesStoredTotal(topic string) {
	producerKafkaMessageStoredTotal.WithLabelValues(topic, "").Inc()
}

func IncreaseKafkaMessagesStoredTotalWithError(topic string, errorMessage string) {
	producerKafkaMessageStoredTotal.WithLabelValues(topic, errorMessage).Inc()
}

func IncreaseProducerTPSExceededCounter(topic string, n int) {
	producerTPSExceededCounter.WithLabelValues(topic).Add(float64(n))
}

func ObserveSendToKafkaTime(topic string, elapsedTime float64) {
	producerSendToKafkaTimeSecond.WithLabelValues(topic).Observe(elapsedTime)
}

func IncreaseProducerKafkaClientFailed() {
	producerKafkaClientFailed.WithLabelValues().Inc()
}
