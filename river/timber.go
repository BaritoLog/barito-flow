package river

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
)

// Timber
type Timber struct {
	Location  string      `json:"location"`
	Tag       string      `json:"tag"`
	Message   string      `json:"message"`
	Timestamp time.Time   `json:"@timestamp"`
	Trail     BaritoTrail `json:"barito_trail"`
}

// BaritoTrail
type BaritoTrail struct {
	receiver  ReceiverTrail  `json:"receiver"`
	forwarder ForwarderTrail `json:"forwarder"`
}

// ReceiverTrail
type ReceiverTrail struct {
	EndPointURL             string    `json:"end_point_url"`
	ReceivedAt              time.Time `json:"received_at"`
	GenerateTimestampReason string    `json:"generate_timestamp_reason"`
}

// ForwarderTrail
type ForwarderTrail struct {
	ForwardedAt time.Time `json:"forwarded_at"`
}

// NewTimberFromRequest create timber instance from http request
func NewTimberFromRequest(req *http.Request) Timber {

	params := mux.Vars(req)
	//streamId := params["stream_id"]
	//storeId := params["store_id"]
	//forwarderId := params["forwarder_id"]
	//clientId := params["client_id"]
	topic := params["topic"]

	body, _ := ioutil.ReadAll(req.Body)

	timber := Timber{
		Location: topic,
		Message:  string(body),
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
