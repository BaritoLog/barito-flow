package receiver

import (
	"net/http"
	"strings"

	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/BaritoLog/go-boilerplate/app"
	"github.com/BaritoLog/go-boilerplate/httpkit"
	"github.com/BaritoLog/go-boilerplate/httpkit/heartbeat"
	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
)

// Context of receiver part
type Context interface {
	Init(config app.Configuration) (err error)
	Run() (err error)
}

// receiver implementation
type context struct {
	server   httpkit.Server
	producer sarama.SyncProducer
}

// NewLogstoreReceiver
func NewContext() Context {
	return &context{}
}

// Init
func (c *context) Init(config app.Configuration) (err error) {

	conf := config.(Configuration)
	fmt.Printf("init context\n%+v\n", conf)

	brokers := strings.Split(conf.kafkaBrokers, ",")
	c.producer, err = sarama.NewSyncProducer(brokers, c.kafkaConfig())
	if err != nil {
		return
	}

	c.server = &http.Server{
		Addr:    conf.addr,
		Handler: c.router(),
	}

	return
}

// Run
func (c *context) Run() (err error) {
	err = c.server.ListenAndServe()
	return
}

func (c *context) router() (router *mux.Router) {
	router = mux.NewRouter()
	router.HandleFunc("/str/{stream_id}/st/{store_id}/fw/{forwarder_id}/cl/{client_id}/produce/{topic}", c.produceHandler).Methods("POST")
	router.HandleFunc("/check-health", heartbeat.Handler)

	return
}

func (c *context) kafkaConfig() (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	return
}

func (c *context) produceHandler(writer http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	// TODO: parse barito params and do something usefule
	//streamId := params["stream_id"]
	//storeId := params["store_id"]
	//forwarderId := params["forwarder_id"]
	//clientId := params["client_id"]

	topic := params["topic"]
	body, _ := ioutil.ReadAll(req.Body)

	message := string(body)
	l := log.New(os.Stdout, "BARITO-RECEIVER : ", 0)

	err := c.ProduceMessage(topic, message)

	if err != nil {
		l.Printf("Failed to produce:, %s", err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	} else {
		l.Printf("Produce to kafka with topic %s", topic)
		writer.WriteHeader(http.StatusOK)
	}

}

// Implementation of SendMessage
func (c *context) ProduceMessage(topic string, message string) error {
	m := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	_, _, err := c.producer.SendMessage(m)

	return err
}
