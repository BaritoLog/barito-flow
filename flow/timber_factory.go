package flow

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
)

const (
	JsonParseError = errkit.Error("JSON Parse Error")
)

// NewTimberFromRequest create timber instance from http request
func NewTimberFromRequest(req *http.Request) (timber Timber, err error) {

	body, _ := ioutil.ReadAll(req.Body)
	err = json.Unmarshal(body, &timber)
	if err != nil {
		err = errkit.Concat(JsonParseError, err)
		return
	}

	if timber.Timestamp() == "" {
		timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
	}

	return
}

// NewTimberFromKafkaMessage create timber instance from kafka message
func NewTimberFromKafkaMessage(message *sarama.ConsumerMessage) (timber Timber, err error) {

	err = json.Unmarshal(message.Value, &timber)
	if err != nil {
		err = errkit.Concat(JsonParseError, err)
		return
	}

	if timber.Timestamp() == "" {
		timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
	}

	return
}

// ConvertToKafkaMessage will convert timber to sarama producer message for kafka
func ConvertToKafkaMessage(timber Timber, topic string) *sarama.ProducerMessage {
	b, _ := json.Marshal(timber)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
}
