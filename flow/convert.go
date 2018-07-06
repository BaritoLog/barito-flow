package flow

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
)

const (
	JsonParseError = errkit.Error("JSON Parse Error")
)

func ConvertBytesToTimber(data []byte) (timber Timber, err error) {
	err = json.Unmarshal(data, &timber)
	if err != nil {
		err = errkit.Concat(JsonParseError, err)
		return
	}

	err = timber.InitContext()
	if err != nil {
		return
	}

	if timber.Timestamp() == "" {
		timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
	}

	return
}

// NewTimberFromRequest create timber instance from http request
func ConvertRequestToTimber(req *http.Request) (Timber, error) {
	body, _ := ioutil.ReadAll(req.Body)
	return ConvertBytesToTimber(body)
}

// NewTimberFromKafkaMessage create timber instance from kafka message
func ConvertKafkaMessageToTimber(message *sarama.ConsumerMessage) (timber Timber, err error) {
	return ConvertBytesToTimber(message.Value)
}

// ConvertToKafkaMessage will convert timber to sarama producer message for kafka
func ConvertTimberToKafkaMessage(timber Timber, topic string) *sarama.ProducerMessage {
	b, _ := json.Marshal(timber)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
}

func ConvertTimberToElasticDocument(timber Timber) map[string]interface{} {
	doc := make(map[string]interface{})
	for k, v := range timber {
		doc[k] = v
	}

	delete(doc, "_ctx")

	return doc
}

func ConvertMapToTimberContext(m map[string]interface{}) (ctx *TimberContext, err error) {
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

	ctx = &TimberContext{
		KafkaTopic:             kafkaTopic,
		KafkaPartition:         int32(kafkaPartition),
		KafkaReplicationFactor: int16(kafkaReplicationFactor),
		ESIndexPrefix:          esIndexPrefix,
		ESDocumentType:         esDocumentType,
	}

	return
}
