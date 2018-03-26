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
	WarnNoMessage   = "No @message"
	WarnNoTimestamp = "No @timestamp"
)

// Timber
type Timber struct {
	Location      string        `json:"location"`
	Tag           string        `json:"tag"`
	Message       string        `json:"@message"`
	Timestamp     time.Time     `json:"@timestamp"`
	ReceiverTrail ReceiverTrail `json:"barito_receiver_trail"`
}

// ReceiverTrail
type ReceiverTrail struct {
	URLPath    string    `json:"url_path"`
	ReceivedAt time.Time `json:"received_at"`
	Warnings   []string  `json:"warnings"`
}

// ForwarderTrail
type ForwarderTrail struct {
	ForwardedAt time.Time `json:"forwarded_at"`
}

// NewTimberFromRequest create timber instance from http request
func NewTimberFromRequest(req *http.Request) Timber {

	var warnings []string

	topic := httpkit.PathParameter(req.URL.Path, "produce")
	body, _ := ioutil.ReadAll(req.Body)

	var timber Timber
	json.Unmarshal(body, &timber)

	if timber.Message == "" {
		timber.Message = string(body)
		warnings = append(warnings, WarnNoMessage)
	}

	if timber.Timestamp.IsZero() {
		timber.Timestamp = time.Now()
		warnings = append(warnings, WarnNoTimestamp)
	}

	timber.Location = topic
	timber.ReceiverTrail = ReceiverTrail{
		URLPath:    req.URL.Path,
		ReceivedAt: time.Now(),
		Warnings:   warnings,
	}

	return timber
}

func NewTimberFromKafkaMessage(message *sarama.ConsumerMessage) Timber {
	timber := Timber{
		Location: message.Topic,
		Message:  string(message.Value),
	}

	return timber
}

// ConvertToKafkaMessage will convert timber to sarama producer message for kafka
func ConvertToKafkaMessage(timber Timber) *sarama.ProducerMessage {
	message := &sarama.ProducerMessage{
		Topic: timber.Location,
		Value: sarama.ByteEncoder(timber.Message),
	}
	return message
}

func ConvertToElasticMessage(timber Timber) map[string]interface{} {
	var message map[string]interface{}
	err := json.Unmarshal([]byte(timber.Message), &message)
	if err != nil {
		message["data"] = timber.Message
	}

	return message
}
