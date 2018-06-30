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

func TestNewTimberFromBytes_GenerateTimestamp(t *testing.T) {
	timekit.FreezeUTC("2018-06-06T12:12:12Z")
	defer timekit.Unfreeze()

	timber, err := NewTimberFromBytes([]byte(`{"hello":"world", "_ctx": {"kafka_topic": "some_topic"}}`))
	FatalIfError(t, err)
	FatalIf(t, timber.Timestamp() != "2018-06-06T12:12:12Z", "wrong timber.Timestamp()")
}

func TestNewTimberFromBytes_JsonParseError(t *testing.T) {
	_, err := NewTimberFromBytes([]byte(`invalid_json`))
	FatalIfWrongError(t, err, string(JsonParseError))
}

func TestNewTimberFromBytes_MissingContext(t *testing.T) {
	_, err := NewTimberFromBytes([]byte(`{"hello":"world"}`))
	FatalIfWrongError(t, err, string(MissingContextError))
}

func TestNewTimberFromBytes_InvalidContext(t *testing.T) {
	_, err := NewTimberFromBytes([]byte(`{"_ctx":{}}`))
	FatalIfWrongError(t, err, "Invalid Context Error: kafka_topic is missing")
}

func TestNewTimberFromRequest(t *testing.T) {

	body := strings.NewReader(`{
    "message":"hello world", 
    "id": "0012", 
    "@timestamp":"2009-11-10T23:00:00Z",
		"_ctx": {
			"kafka_topic": "some_topic"
		}
  }`)

	req, err := http.NewRequest("POST", "/", body)
	FatalIfError(t, err)

	timber, err := NewTimberFromRequest(req)

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
			"_ctx": {
				"kafka_topic": "some_topic"
			}
    }`),
	}

	timber, err := NewTimberFromKafkaMessage(message)
	FatalIfError(t, err)
	FatalIf(t, timber["message"] != "some-message", "Wrong timber[message]")
	FatalIf(t, timber.Timestamp() != "2009-11-10T23:00:00Z", "Wrong message: %v", timber.Timestamp)
}

func TestNewTimberFromKafka_InvalidMessage(t *testing.T) {
	message := &sarama.ConsumerMessage{
		Topic: "some-topic",
		Value: []byte(`invalid_message`),
	}

	_, err := NewTimberFromKafkaMessage(message)
	FatalIfWrongError(t, err, string(JsonParseError))
}

func TestConvertToKafkaMessage(t *testing.T) {
	topic := "some-topic"

	timber := Timber{}
	timber.SetTimestamp("2018-03-10T23:00:00Z")

	message := ConvertToKafkaMessage(timber, topic)
	FatalIf(t, message.Topic != topic, "%s != %s", message.Topic, topic)

	get, _ := message.Value.Encode()
	expected, _ := json.Marshal(timber)
	FatalIf(t, string(get) != string(expected), "Wrong message value")
}
