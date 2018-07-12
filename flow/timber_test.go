package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestMapToContext(t *testing.T) {

	ctx, err := mapToContext(sampleContextMap())

	FatalIfError(t, err)
	FatalIf(t, ctx.KafkaTopic != "some-topic", "wrong kafka_topic")
	FatalIf(t, ctx.KafkaPartition != 3, "wrong kafka_partition")
	FatalIf(t, ctx.KafkaReplicationFactor != 1, "wrong kafka_replication_factor")
	FatalIf(t, ctx.ESIndexPrefix != "some-prefix", "wrong es_index_prefix")
	FatalIf(t, ctx.ESDocumentType != "some-type", "wrong es_document_type")
}

func TestMapToContext_KafkaTopicIsMissing(t *testing.T) {
	ctxMap := sampleContextMap()
	delete(ctxMap, "kafka_topic")

	_, err := mapToContext(ctxMap)

	FatalIfWrongError(t, err, "kafka_topic is missing")
}

func TestMapToContext_ESIndexPrefixIsMissing(t *testing.T) {
	ctxMap := sampleContextMap()
	delete(ctxMap, "es_index_prefix")

	_, err := mapToContext(ctxMap)

	FatalIfWrongError(t, err, "es_index_prefix is missing")
}

func TestMapToContext_ESDocumentTypeIsMissing(t *testing.T) {
	ctxMap := sampleContextMap()
	delete(ctxMap, "es_document_type")

	_, err := mapToContext(ctxMap)

	FatalIfWrongError(t, err, "es_document_type is missing")
}

func TestMapToContext_KafkaPartitionIsMissing(t *testing.T) {
	ctxMap := sampleContextMap()
	delete(ctxMap, "kafka_partition")

	_, err := mapToContext(ctxMap)

	FatalIfWrongError(t, err, "kafka_partition is missing")
}

func TestMapToContext_KafkaReplicationFactorIsMissing(t *testing.T) {
	ctxMap := sampleContextMap()
	delete(ctxMap, "kafka_replication_factor")

	_, err := mapToContext(ctxMap)

	FatalIfWrongError(t, err, "kafka_replication_factor is missing")
}

func sampleContextMap() map[string]interface{} {
	return map[string]interface{}{
		"kafka_topic":              "some-topic",
		"kafka_partition":          3.0,
		"kafka_replication_factor": 1.0,
		"es_index_prefix":          "some-prefix",
		"es_document_type":         "some-type",
		"app_max_tps":              10.0,
	}
}
