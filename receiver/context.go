package receiver

import (
	"net/http"

	"github.com/BaritoLog/go-boilerplate/app"
	"github.com/BaritoLog/go-boilerplate/httpkit"
	"github.com/BaritoLog/go-boilerplate/httpkit/heartbeat"
	"github.com/gorilla/mux"
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

func (c *context) testHandler(writer http.ResponseWriter, req *http.Request) {

	writer.WriteHeader(http.StatusOK)
}

func (c *context) produceHandler(writer http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	streamId := params["stream_id"]
	storeId := params["store_id"]
	forwarderId := params["forwarder_id"]
	clientId := params["client_id"]
	topic := params["topic"]
	body, _ := ioutil.ReadAll(req.Body)

	l := log.New(os.Stdout, "BARITO-RECEIVER : ", 0)

	l.Printf("stream %s store %s forwarder %s client %s topic %s",streamId, storeId, forwarderId, clientId, topic)
	l.Printf("message %s", string(body))
	// TODO: parsing url to get group_id/store_id/service_id

	writer.WriteHeader(http.StatusOK)
}
