package flow

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
)

func TestConvertBytesToTimber_GenerateTimestamp(t *testing.T) {
	timekit.FreezeUTC("2018-06-06T12:12:12Z")
	defer timekit.Unfreeze()

	timber, err := ConvertBytesToTimber([]byte(`{"hello":"world", "_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	FatalIfError(t, err)
	FatalIf(t, timber.Timestamp() != "2018-06-06T12:12:12Z", "wrong timber.Timestamp()")
}

func TestConvertBytesToTimber_JsonParseError(t *testing.T) {
	_, err := ConvertBytesToTimber([]byte(`invalid_json`))
	FatalIfWrongError(t, err, string(JsonParseError))
}

func TestConvertBytesToTimber_MissingContext(t *testing.T) {
	_, err := ConvertBytesToTimber([]byte(`{"hello":"world"}`))
	FatalIfWrongError(t, err, string(MissingContextError))
}

func TestConvertBytesToTimber_InvalidContext(t *testing.T) {
	_, err := ConvertBytesToTimber([]byte(`{"_ctx":{}}`))
	FatalIfWrongError(t, err, "Invalid Context Error: kafka_topic is missing")
}

func TestConvertRequestToTimber(t *testing.T) {

	body := strings.NewReader(`{
    "message":"hello world", 
    "id": "0012", 
    "@timestamp":"2009-11-10T23:00:00Z",
		"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}
  }`)

	req, err := http.NewRequest("POST", "/", body)
	FatalIfError(t, err)

	timber, err := ConvertRequestToTimber(req)
	FatalIfError(t, err)

	FatalIf(t, timber["message"] != "hello world", "Wrong timber.message")
	FatalIf(t, timber["id"] != "0012", "Wrong timber.id")
	FatalIf(t, timber.Timestamp() != "2009-11-10T23:00:00Z", "Wrong timestamp")
}

func TestNewTimberFromKafka(t *testing.T) {

	message := &sarama.ConsumerMessage{
		Topic: "some-topic",
		Value: []byte(`{
      "location": "some-location", 
      "message":"some-message", 
      "@timestamp":"2009-11-10T23:00:00Z",
			"_ctx": {"kafka_topic": "some_topic","es_index_prefix": "some-type","es_document_type": "some-type"}
    }`),
	}

	timber, err := ConvertKafkaMessageToTimber(message)
	FatalIfError(t, err)
	FatalIf(t, timber["message"] != "some-message", "Wrong timber[message]")
	FatalIf(t, timber.Timestamp() != "2009-11-10T23:00:00Z", "Wrong message: %v", timber.Timestamp)
}

func TestConvertToKafkaMessage(t *testing.T) {
	topic := "some-topic"

	timber := Timber{}
	timber.SetTimestamp("2018-03-10T23:00:00Z")

	message := ConvertTimberToKafkaMessage(timber, topic)
	FatalIf(t, message.Topic != topic, "%s != %s", message.Topic, topic)

	get, _ := message.Value.Encode()
	expected, _ := json.Marshal(timber)
	FatalIf(t, string(get) != string(expected), "Wrong message value")
}

func TestConvertMapToTimberContext(t *testing.T) {
	ctx, err := ConvertMapToTimberContext(map[string]interface{}{
		"kafka_topic":      "some-topic",
		"es_index_prefix":  "some-prefix",
		"es_document_type": "some-type",
	})

	FatalIfError(t, err)
	FatalIf(t, ctx.KafkaTopic != "some-topic", "wrong kafka_topic")
	FatalIf(t, ctx.ESIndexPrefix != "some-prefix", "wrong es_index_prefix")
	FatalIf(t, ctx.ESDocumentType != "some-type", "wrong es_document_type")
}

func TestConvertMapToTimberContext_KafkaTopicIsMissing(t *testing.T) {
	_, err := ConvertMapToTimberContext(map[string]interface{}{
		"es_index_prefix":  "some-prefix",
		"es_document_type": "some-type",
	})

	FatalIfWrongError(t, err, "kafka_topic is missing")
}

func TestConvertMapToTimberContext_ESIndexPrefixIsMissing(t *testing.T) {
	_, err := ConvertMapToTimberContext(map[string]interface{}{
		"kafka_topic":      "some-topic",
		"es_document_type": "some-type",
	})

	FatalIfWrongError(t, err, "es_index_prefix is missing")
}

func TestConvertMapToTimberContext_ESDocumentTypeIsMissing(t *testing.T) {
	_, err := ConvertMapToTimberContext(map[string]interface{}{
		"kafka_topic":     "some-topic",
		"es_index_prefix": "some-prefix",
	})

	FatalIfWrongError(t, err, "es_document_type is missing")
}

func TestConvertTimberToElasticDocument(t *testing.T) {
	timber := NewTimber()
	timber["hello"] = "world"
	timber.SetContext(&TimberContext{
		KafkaTopic:     "some-topic",
		ESIndexPrefix:  "some-prefix",
		ESDocumentType: "some-type",
	})

	document := ConvertTimberToElasticDocument(timber)
	FatalIf(t, len(document) != 1, "wrong document size")
	FatalIf(t, timber["hello"] != "world", "wrong document.hello")
}
