package flow

import "fmt"

type TimberContext struct {
	KafkaTopic string `json:"kafka_topic"`
}

func NewTimberContextFromMap(m map[string]interface{}) (ctx *TimberContext, err error) {
	kafkaTopic, ok := m["kafka_topic"].(string)
	if !ok {
		err = fmt.Errorf("kafka_topic is missing")
		return
	}

	ctx = &TimberContext{
		KafkaTopic: kafkaTopic,
	}

	return
}
