package receiver

import (
	"logstore/common/app"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/imantung/go-boilerplate/http/heartbeat"
)

// Context of receiver part
type Context interface {
	Init(config app.Configuration) (err error)
	Run() (err error)
	Configuration() Configuration
}

// receiver implementation
type context struct {
	conf Configuration
}

// NewLogstoreReceiver
func NewContext() Context {
	return &context{}
}

// Init
func (c *context) Init(config app.Configuration) (err error) {
	c.conf = config.(Configuration)

	return
}

// Run
func (c *context) Run() (err error) {

	router := mux.NewRouter()
	router.HandleFunc("/produce", c.ProduceHandler).Methods("POST")
	router.HandleFunc("/check-health", heartbeat.Handler)

	err = http.ListenAndServe(c.conf.addr, router)

	return
}

// Configuration
func (c *context) Configuration() Configuration {
	return c.conf

}

// ProducerHandler
func (c *context) ProduceHandler(respWriter http.ResponseWriter, req *http.Request) {
	// body, _ := ioutil.ReadAll(req.Body)
	// TODO: parsing url to get group_id/store_id/service_id

}
