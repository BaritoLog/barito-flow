package prome

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var consumerLogStoredCounter *prometheus.CounterVec
var consumerBulkProcessTimeSecond prometheus.Summary
var consumerKafkaMessagesIncomingCounter *prometheus.CounterVec
var producerKafkaMessageStoredTotal *prometheus.CounterVec
var producerTPSExceededCounter *prometheus.CounterVec

func InitConsumerInstrumentation() {
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
}

func IncreaseLogStoredCounter(index string, result string, status int, error string) {
	consumerLogStoredCounter.WithLabelValues(index, result, strconv.Itoa(status), error).Inc()
}

func IncreaseKafkaMessagesIncoming(topic string) {
	consumerKafkaMessagesIncomingCounter.WithLabelValues(topic).Inc()
}

func ObserveBulkProcessTime(elapsedTime float64) {
	consumerBulkProcessTimeSecond.Observe(elapsedTime)
}

func IncreaseKafkaMessagesStoredTotal(topic string) {
	producerKafkaMessageStoredTotal.WithLabelValues(topic, "").Inc()
}

func IncreaseKafkaMessagesStoredTotalWithError(topic string, errorType string) {
	producerKafkaMessageStoredTotal.WithLabelValues(topic, errorType).Inc()
}

func IncreaseProducerTPSExceededCounter(topic string) {
	producerTPSExceededCounter.WithLabelValues(topic).Inc()
}
