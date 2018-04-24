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
	ClientTrail    ClientTrail    `json:"client_trail"`
	ReceiverTrail  ReceiverTrail  `json:"receiver_trail"`
	ForwarderTrail ForwarderTrail `json:"forwarder_trail"`
}

// ClientTrail
type ClientTrail struct {
	IsK8s  bool     `json:"is_k8s"`
	SentAt string   `json:"sent_at"`
	Hints  []string `json:"hints"`
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
		timber.SetMessage(string(body))
		hints = append(hints, HintNoMessage)
	}

	if timber.Timestamp == "" {
		timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
		hints = append(hints, HintNoTimestamp)
	}

	timber.SetLocation(httpkit.PathParameter(req.URL.Path, "produce"))
	timber.SetReceiverTrail(ReceiverTrail{
		URLPath:    req.URL.Path,
		ReceivedAt: time.Now().UTC().Format(time.RFC3339),
		Hints:      hints,
	})

	return timber
}

// NewTimberFromKafkaMessage create timber instance from kafka message
func NewTimberFromKafkaMessage(message *sarama.ConsumerMessage) Timber {

	var hints []string

	var timber Timber
	json.Unmarshal(message.Value, &timber)

	if timber.Location == "" {
		timber.SetLocation(message.Topic)
		hints = append(hints, HintNoLocation)
	}

	if timber.Message == "" {
		timber.SetMessage(string(message.Value))
		hints = append(hints, HintNoMessage)
	}

	if timber.Timestamp == "" {
		timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
		hints = append(hints, HintNoTimestamp)
	}

	timber.SetForwarderTrail(ForwarderTrail{
		ForwardedAt: time.Now().UTC().Format(time.RFC3339),
		Hints:       hints,
	})

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

func (t *Timber) SetLocation(location string) {
	// TODO: `json:"location"`
	t.Location = location
}

func (t *Timber) SetMessage(message string) {
	// TODO: `json:"@message"`
	t.Message = message
}

func (t *Timber) SetTimestamp(timestamp string) {
	// TODO: `json:"@timestamp"`
	t.Timestamp = timestamp
}

func (t *Timber) SetReceiverTrail(receiverTrail ReceiverTrail) {
	// TODO: `json:"receiver_trail"`
	t.ReceiverTrail = receiverTrail
}

func (t *Timber) SetForwarderTrail(forwarderTrail ForwarderTrail) {
	// TODO: `json:"forwarder_trail"`
	t.ForwarderTrail = forwarderTrail
}
