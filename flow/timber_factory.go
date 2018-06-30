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
	JsonParseError      = errkit.Error("JSON Parse Error")
	InvalidContextError = errkit.Error("Invalid Context Error")
	MissingContextError = errkit.Error("Missing Context Error")
)

func NewTimberFromBytes(data []byte) (timber Timber, err error) {
	err = json.Unmarshal(data, &timber)
	if err != nil {
		err = errkit.Concat(JsonParseError, err)
		return
	}

	ctxMap, ok := timber["_ctx"].(map[string]interface{})
	if !ok {
		err = MissingContextError
		return
	}

	ctx, err := NewTimberContextFromMap(ctxMap)
	if err != nil {
		err = errkit.Concat(InvalidContextError, err)
		return
	}

	timber["_ctx"] = ctx

	if timber.Timestamp() == "" {
		timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
	}

	return
}

// NewTimberFromRequest create timber instance from http request
func NewTimberFromRequest(req *http.Request) (Timber, error) {
	body, _ := ioutil.ReadAll(req.Body)
	return NewTimberFromBytes(body)
}

// NewTimberFromKafkaMessage create timber instance from kafka message
func NewTimberFromKafkaMessage(message *sarama.ConsumerMessage) (timber Timber, err error) {
	return NewTimberFromBytes(message.Value)
}

// ConvertToKafkaMessage will convert timber to sarama producer message for kafka
func ConvertToKafkaMessage(timber Timber, topic string) *sarama.ProducerMessage {
	b, _ := json.Marshal(timber)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
}
