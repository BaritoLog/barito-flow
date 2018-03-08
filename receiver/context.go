package receiver

import (
	"net/http"

	"github.com/BaritoLog/go-boilerplate/app"
	"github.com/BaritoLog/go-boilerplate/httpkit"
	"github.com/BaritoLog/go-boilerplate/httpkit/heartbeat"
	"github.com/gorilla/mux"
	"github.com/BaritoLog/barito-flow/kafka"
	"io/ioutil"
	"log"
	"os"
	"fmt"
)

// Context of receiver part
type Context interface {
	Init(config app.Configuration) (err error)
	Run() (err error)
}

// receiver implementation
type context struct {
	server httpkit.Server
	kafkaProducer kafka.Producer
}

// NewLogstoreReceiver
func NewContext() Context {
	return &context{}
}

// Init
func (c *context) Init(config app.Configuration) (err error) {
	fmt.Println("init context")
	conf := config.(Configuration)

	c.server = &http.Server{
		Addr:    conf.addr,
		Handler: c.router(),
	}

	kafka := kafka.NewKafka(conf.kafkaBrokers)
	producer, err := kafka.Producer()
	if err != nil {
		fmt.Sprintf("%s", err)
		return
	}
	c.kafkaProducer = producer

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

	err := c.kafkaProducer.SendMessage(topic, message)

	if err != nil {
		l.Printf("Failed to produce:, %s", err)
		http.Error(writer, err.Error(),http.StatusInternalServerError)
		return
	} else {
		l.Printf("Produce to kafka with topic %s", topic)
		writer.WriteHeader(http.StatusOK)
	}

}

