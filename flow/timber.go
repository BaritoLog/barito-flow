package flow

import "github.com/BaritoLog/go-boilerplate/errkit"

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

	ctx, err := ConvertMapToTimberContext(ctxMap)
	if err != nil {
		err = errkit.Concat(InvalidContextError, err)
		return
	}

	t.SetContext(ctx)
	return
}
