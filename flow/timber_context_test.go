package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestTimberContext(t *testing.T) {
	ctx, err := NewTimberContextFromMap(map[string]interface{}{
		"kafka_topic":      "some-topic",
		"es_index_prefix":  "some-prefix",
		"es_document_type": "some-type",
	})

	FatalIfError(t, err)
	FatalIf(t, ctx.KafkaTopic != "some-topic", "wrong kafka_topic")
	FatalIf(t, ctx.ESIndexPrefix != "some-prefix", "wrong es_index_prefix")
	FatalIf(t, ctx.ESDocumentType != "some-type", "wrong es_document_type")
}

func TestTimberContext_KafkaTopicIsMissing(t *testing.T) {
	_, err := NewTimberContextFromMap(map[string]interface{}{
		"es_index_prefix":  "some-prefix",
		"es_document_type": "some-type",
	})

	FatalIfWrongError(t, err, "kafka_topic is missing")
}

func TestTimberContext_ESIndexPrefixIsMissing(t *testing.T) {
	_, err := NewTimberContextFromMap(map[string]interface{}{
		"kafka_topic":      "some-topic",
		"es_document_type": "some-type",
	})

	FatalIfWrongError(t, err, "es_index_prefix is missing")
}

func TestTimberContext_ESDocumentTypeIsMissing(t *testing.T) {
	_, err := NewTimberContextFromMap(map[string]interface{}{
		"kafka_topic":     "some-topic",
		"es_index_prefix": "some-prefix",
	})

	FatalIfWrongError(t, err, "es_document_type is missing")

}
