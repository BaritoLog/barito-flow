package river

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
)

// TODO: standarize kafka message & elastic message

// Timber
type Timber struct {
	Location string
	Data     []byte
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
		Data:     body,
	}

	return timber
}

func NewTimberFromKafkaMessage(message *sarama.ConsumerMessage) Timber {
	timber := Timber{
		Location: message.Topic,
		Data:     message.Value,
	}

	return timber
}

// ConvertToKafkaMessage will convert timber to sarama producer message for kafka
func ConvertToKafkaMessage(timber Timber) *sarama.ProducerMessage {
	message := &sarama.ProducerMessage{
		Topic: timber.Location,
		Value: sarama.ByteEncoder(timber.Data),
	}
	return message
}

func ConvertToElasticMessage(timber Timber) map[string]interface{} {
	var message map[string]interface{}
	err := json.Unmarshal(timber.Data, &message)
	if err != nil {
		message["data"] = timber.Data
	}

	return message
}
