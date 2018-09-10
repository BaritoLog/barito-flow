package flow

import (
	"fmt"

	"github.com/BaritoLog/go-boilerplate/errkit"
)

const (
	InvalidContextError = errkit.Error("Invalid Context Error")
	MissingContextError = errkit.Error("Missing Context Error")
)

// Timber
type Timber map[string]interface{}

type TimberContext struct {
	KafkaTopic             string `json:"kafka_topic"`
	KafkaPartition         int32  `json:"kafka_partition"`
	KafkaReplicationFactor int16  `json:"kafka_replication_factor"`
	ESIndexPrefix          string `json:"es_index_prefix"`
	ESDocumentType         string `json:"es_document_type"`
	AppMaxTPS              int    `json:"app_max_tps"`
	AppSecret              string `json:"app_secret"`
}

func NewTimber() Timber {
	timber := make(map[string]interface{})
	return timber
}

func (t Timber) SetTimestamp(timestamp string) {
	t["@timestamp"] = timestamp
}

func (t Timber) Timestamp() (s string) {
	s, _ = t["@timestamp"].(string)
	return
}

func (t Timber) Context() (ctx *TimberContext) {
	ctx, _ = t["_ctx"].(*TimberContext)
	return
}

func (t Timber) SetContext(ctx *TimberContext) {
	t["_ctx"] = ctx
}

func (t Timber) InitContext() (err error) {
	ctxMap, ok := t["_ctx"].(map[string]interface{})
	if !ok {
		err = MissingContextError
		return
	}

	ctx, err := mapToContext(ctxMap)
	if err != nil {
		err = errkit.Concat(InvalidContextError, err)
		return
	}

	t.SetContext(ctx)
	return
}

func mapToContext(m map[string]interface{}) (ctx *TimberContext, err error) {
	kafkaTopic, ok := m["kafka_topic"].(string)
	if !ok {
		err = fmt.Errorf("kafka_topic is missing")
		return
	}

	esIndexPrefix, ok := m["es_index_prefix"].(string)
	if !ok {
		err = fmt.Errorf("es_index_prefix is missing")
		return
	}

	esDocumentType, ok := m["es_document_type"].(string)
	if !ok {
		err = fmt.Errorf("es_document_type is missing")
		return
	}

	kafkaPartition, ok := m["kafka_partition"].(float64)
	if !ok {
		err = fmt.Errorf("kafka_partition is missing")
		return
	}

	kafkaReplicationFactor, ok := m["kafka_replication_factor"].(float64)
	if !ok {
		err = fmt.Errorf("kafka_replication_factor is missing")
		return
	}

	appMaxTPS, ok := m["app_max_tps"].(float64)
	if !ok {
		err = fmt.Errorf("app_max_tps is missing")
		return
	}

	appSecret, ok := m["app_secret"].(string)
	if !ok {
		err = fmt.Errorf("app_secret is missing")
		return
	}

	ctx = &TimberContext{
		KafkaTopic:             kafkaTopic,
		KafkaPartition:         int32(kafkaPartition),
		KafkaReplicationFactor: int16(kafkaReplicationFactor),
		ESIndexPrefix:          esIndexPrefix,
		ESDocumentType:         esDocumentType,
		AppMaxTPS:              int(appMaxTPS),
		AppSecret:              appSecret,
	}

	return
}
