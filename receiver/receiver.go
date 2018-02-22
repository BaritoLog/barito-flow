package receiver

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/urfave/cli"
)

const (
	Version = "0.1"
)

func Start(c *cli.Context) (err error) {
	fmt.Printf("Reciever v%s\n", Version)

	r := mux.NewRouter()
	r.HandleFunc("/", handler).Methods("POST")

	err = http.ListenAndServe(":8080", r)
	return
}

func handler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		http.Error(w, "Body is nil", http.StatusUnprocessableEntity)
		return
	}

	fmt.Printf("%s", string(body))
}
