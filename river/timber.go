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
type Timber map[string]interface{}

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

func NewTimber() Timber {
	timber := make(map[string]interface{})
	return timber
}

// NewTimberFromRequest create timber instance from http request
func NewTimberFromRequest(req *http.Request) Timber {

	var hints []string

	body, _ := ioutil.ReadAll(req.Body)

	timber := NewTimber()
	json.Unmarshal(body, &timber)

	if timber.Message() == "" {
		timber.SetMessage(string(body))
		hints = append(hints, HintNoMessage)
	}

	if timber.Timestamp() == "" {
		timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
		hints = append(hints, HintNoTimestamp)
	}

	timber.SetLocation(httpkit.PathParameter(req.URL.Path, "produce"))
	timber.SetReceiverTrail(&ReceiverTrail{
		URLPath:    req.URL.Path,
		ReceivedAt: time.Now().UTC().Format(time.RFC3339),
		Hints:      hints,
	})

	return timber
}

// NewTimberFromKafkaMessage create timber instance from kafka message
func NewTimberFromKafkaMessage(message *sarama.ConsumerMessage) Timber {

	var hints []string

	timber := NewTimber()
	json.Unmarshal(message.Value, &timber)

	if timber.Location() == "" {
		timber.SetLocation(message.Topic)
		hints = append(hints, HintNoLocation)
	}

	if timber.Message() == "" {
		timber.SetMessage(string(message.Value))
		hints = append(hints, HintNoMessage)
	}

	if timber.Timestamp() == "" {
		timber.SetTimestamp(time.Now().UTC().Format(time.RFC3339))
		hints = append(hints, HintNoTimestamp)
	}

	timber.SetForwarderTrail(&ForwarderTrail{
		ForwardedAt: time.Now().UTC().Format(time.RFC3339),
		Hints:       hints,
	})

	return timber
}

// ConvertToKafkaMessage will convert timber to sarama producer message for kafka
func ConvertToKafkaMessage(timber Timber, topic string) *sarama.ProducerMessage {
	b, _ := json.Marshal(timber)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
}

func (t Timber) SetLocation(location string) {
	t["location"] = location
}

func (t Timber) Location() (s string) {
	v := t["location"]
	if v != nil {
		s = v.(string)
	}
	return
}

func (t Timber) SetMessage(message string) {
	t["@message"] = message
}

func (t Timber) Message() (s string) {
	v := t["@message"]
	if v != nil {
		s = v.(string)
	}

	return
}

func (t Timber) SetTimestamp(timestamp string) {
	t["@timestamp"] = timestamp
}

func (t Timber) Timestamp() (s string) {
	v := t["@timestamp"]
	if v != nil {
		s = v.(string)
	}

	return
}

func (t Timber) SetReceiverTrail(receiverTrail *ReceiverTrail) {
	t["receiver_trail"] = receiverTrail
}

func (t Timber) ReceiverTrail() (trail *ReceiverTrail) {
	v := t["receiver_trail"]
	if v != nil {
		trail = v.(*ReceiverTrail)
	}

	return
}

func (t Timber) SetForwarderTrail(forwarderTrail *ForwarderTrail) {
	t["forwarder_trail"] = forwarderTrail
}

func (t Timber) ForwarderTrail() (trail *ForwarderTrail) {
	v := t["forwarder_trail"]
	if v != nil {
		trail = v.(*ForwarderTrail)
	}
	return
}
