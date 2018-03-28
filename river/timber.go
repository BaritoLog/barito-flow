package river

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/BaritoLog/go-boilerplate/httpkit"
	"github.com/Shopify/sarama"
)

const (
	HintNoLocation  = "No location"
	HintNoMessage   = "No @message"
	HintNoTimestamp = "No @timestamp"
)

// Timber
type Timber struct {
	Location       string         `json:"location"`
	Tag            string         `json:"tag"`
	Message        string         `json:"@message"`
	Timestamp      string         `json:"@timestamp"`
	ReceiverTrail  ReceiverTrail  `json:"barito_receiver_trail"`
	ForwarderTrail ForwarderTrail `json:"barito_forwarder_trail"`
}

// ReceiverTrail
type ReceiverTrail struct {
	URLPath    string   `json:"url_path"`
	ReceivedAt string   `json:"received_at"`
	Hints      []string `json:"hints"`
}

// ForwarderTrail
type ForwarderTrail struct {
	ForwardedAt string   `json:"forwarded_at"`
	Hints       []string `json:"hints"`
}

// NewTimberFromRequest create timber instance from http request
func NewTimberFromRequest(req *http.Request) Timber {

	var hints []string

	body, _ := ioutil.ReadAll(req.Body)

	var timber Timber
	json.Unmarshal(body, &timber)

	if timber.Message == "" {
		timber.Message = string(body)
		hints = append(hints, HintNoMessage)
	}

	if timber.Timestamp == "" {
		timber.Timestamp = time.Now().UTC().Format(time.RFC3339)
		hints = append(hints, HintNoTimestamp)
	}

	timber.Location = httpkit.PathParameter(req.URL.Path, "produce")
	timber.ReceiverTrail = ReceiverTrail{
		URLPath:    req.URL.Path,
		ReceivedAt: time.Now().UTC().Format(time.RFC3339),
		Hints:      hints,
	}

	return timber
}

// NewTimberFromKafkaMessage create timber instance from kafka message
func NewTimberFromKafkaMessage(message *sarama.ConsumerMessage) Timber {

	var hints []string

	var timber Timber
	json.Unmarshal(message.Value, &timber)

	if timber.Location == "" {
		timber.Location = message.Topic
		hints = append(hints, HintNoLocation)
	}

	if timber.Message == "" {
		timber.Message = string(message.Value)
		hints = append(hints, HintNoMessage)
	}

	if timber.Timestamp == "" {
		timber.Timestamp = time.Now().UTC().Format(time.RFC3339)
		hints = append(hints, HintNoTimestamp)
	}

	timber.ForwarderTrail = ForwarderTrail{
		ForwardedAt: time.Now().UTC().Format(time.RFC3339),
		Hints:       hints,
	}

	return timber
}

// ConvertToKafkaMessage will convert timber to sarama producer message for kafka
func ConvertToKafkaMessage(timber Timber) *sarama.ProducerMessage {
	b, _ := json.Marshal(timber)

	return &sarama.ProducerMessage{
		Topic: timber.Location,
		Value: sarama.ByteEncoder(b),
	}
}
