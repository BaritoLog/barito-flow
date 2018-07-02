package flow

import "fmt"

type TimberContext struct {
	KafkaTopic     string `json:"kafka_topic"`
	ESIndexPrefix  string `json:"es_index_prefix"`
	ESDocumentType string `json:"es_document_type"`
}

func NewTimberContextFromMap(m map[string]interface{}) (ctx *TimberContext, err error) {
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

	ctx = &TimberContext{
		KafkaTopic:     kafkaTopic,
		ESIndexPrefix:  esIndexPrefix,
		ESDocumentType: esDocumentType,
	}

	return
}
