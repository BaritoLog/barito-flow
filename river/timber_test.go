package river

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/BaritoLog/go-boilerplate/strslice"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
)

func TestNewTimberFromRequest(t *testing.T) {

	url := "/str/18/st/1/fw/1/cl/10/produce/kafka-dummy-topic"
	body := strings.NewReader(`{"@message":"hello world", "@timestamp":"2009-11-10T23:00:00Z"}`)

	req, err := http.NewRequest("POST", url, body)
	FatalIfError(t, err)

	timber := NewTimberFromRequest(req)
	FatalIf(t, timber.Location != "kafka-dummy-topic", "Wrong timber location: %s", timber.Location)
	FatalIf(t, timber.Message != "hello world", "Wrong timber message: %s", timber.Message)
	FatalIf(t, !timekit.EqualUTC(timber.Timestamp, "2009-11-10T23:00:00Z"), "Wrong timestamp: %v", timber.Timestamp)

	trail := timber.ReceiverTrail
	FatalIf(t, trail.URLPath != url, "Wrong trail URL Path: %s", trail.URLPath)
	FatalIf(t, trail.ReceivedAt.IsZero(), "Trail received at must be generated")
	FatalIf(t, len(trail.Hints) > 0, "Trail hints must be empty: %v", len(trail.Hints))

}

func TestNewTimberFromRequest_InvalidRequest(t *testing.T) {

	url := "/str/18/st/1/fw/1/cl/10/produce/kafka-dummy-topic"
	body := strings.NewReader(`invalid_request`)

	req, err := http.NewRequest("POST", url, body)
	FatalIfError(t, err)

	timber := NewTimberFromRequest(req)
	FatalIf(t, timber.Message != `invalid_request`,
		"Wrong timber message: %s", timber.Message)

	trail := timber.ReceiverTrail
	hints := []string{HintNoMessage, HintNoTimestamp}
	for _, hint := range hints {
		FatalIf(t, !strslice.Contain(trail.Hints, hint),
			"Trails warning must contain '%s': %v", hint, trail.Hints)
	}

}

func TestNewTimberFromKafka(t *testing.T) {

	message := &sarama.ConsumerMessage{
		Topic: "some-topic",
		Value: []byte(`{"location": "some-location", "@message":"some-message", "@timestamp":"2009-11-10T23:00:00Z"}`),
	}

	timber := NewTimberFromKafkaMessage(message)
	FatalIf(t, timber.Location != "some-location", "Wrong location: %s", timber.Location)
	FatalIf(t, timber.Message != "some-message", "Wrong message: %s", timber.Message)
	FatalIf(t, !timekit.EqualUTC(timber.Timestamp, "2009-11-10T23:00:00Z"), "Wrong message: %v", timber.Timestamp)

	trail := timber.ForwarderTrail
	FatalIf(t, len(trail.Hints) > 0, "Trail hints must be empty: %v", len(trail.Hints))
}

func TestNewTimberFromKafka_InvalidMessage(t *testing.T) {
	message := &sarama.ConsumerMessage{
		Topic: "some-topic",
		Value: []byte(`invalid_message`),
	}

	timber := NewTimberFromKafkaMessage(message)
	FatalIf(t, timber.Location != "some-topic", "Wrong location: %s", timber.Location)
	FatalIf(t, timber.Message != "invalid_message", "Wrong message: %s", timber.Message)
	FatalIf(t, timber.Timestamp.IsZero(), "Timber timestamp can't be zero")

	trail := timber.ForwarderTrail
	hints := []string{HintNoMessage, HintNoLocation, HintNoTimestamp}
	for _, hint := range hints {
		FatalIf(t, !strslice.Contain(trail.Hints, hint),
			"Trail hints must contain '%s': %v", hint, trail.Hints)
	}
}

func TestConvertToKafkaMessage(t *testing.T) {
	timber := Timber{
		Location:  "some-location",
		Message:   "some-message",
		Timestamp: timekit.UTC("2018-03-10T23:00:00Z"),
	}

	message := ConvertToKafkaMessage(timber)
	FatalIf(t, message.Topic != timber.Location, "%s != %s", message.Topic, timber.Location)

	get, _ := message.Value.Encode()
	expected, _ := json.Marshal(timber)
	FatalIf(t, string(get) != string(expected), "Wrong message value")
}
