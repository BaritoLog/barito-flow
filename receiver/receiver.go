package receiver

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/imantung/go-boilerplate/http/heartbeat"
)

// interface of receiver
type Receiver interface {
	Start() (err error)
}

// receiver implementation
type receiver struct {
	addr string
}

// NewLogstoreReceiver
func NewLogstoreReceiver(addr string) Receiver {
	return &receiver{
		addr: addr,
	}
}

// Start
func (r receiver) Start() (err error) {

	router := mux.NewRouter()
	router.HandleFunc("/produce", r.ProduceHandler).Methods("POST")
	router.HandleFunc("/check-health", heartbeat.Handler)

	err = http.ListenAndServe(r.addr, router)
	return
}

// ProducerHandler
func (r receiver) ProduceHandler(respWriter http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)

	if err != nil {
		http.Error(respWriter, "Body is nil", http.StatusUnprocessableEntity)
		return
	}

	fmt.Printf("%s", string(body))

}
