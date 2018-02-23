package receiver

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
)

type Receiver interface {
	Start() (err error)
}

type LogstoreReceiver struct {
}

func NewLogstoreReceiver() (receiver *LogstoreReceiver) {
	return &LogstoreReceiver{}
}

func (r LogstoreReceiver) Start() (err error) {

	router := mux.NewRouter()
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)

		if err != nil {
			http.Error(w, "Body is nil", http.StatusUnprocessableEntity)
			return
		}

		fmt.Printf("%s", string(body))
	}).Methods("POST")

	err = http.ListenAndServe(":8080", router)
	return
}
