package receiver

import (
	"barito-agent/common/app"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/imantung/go-boilerplate/httpkit"
	"github.com/imantung/go-boilerplate/httpkit/heartbeat"
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
	router.HandleFunc("/produce", c.produceHandler).Methods("POST")
	router.HandleFunc("/check-health", heartbeat.Handler)

	return
}

func (c *context) produceHandler(writer http.ResponseWriter, req *http.Request) {
	// body, _ := ioutil.ReadAll(req.Body)
	// TODO: parsing url to get group_id/store_id/service_id

	writer.WriteHeader(http.StatusOK)
}
