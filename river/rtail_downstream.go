package river

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/gorilla/mux"
	"golang.org/x/net/http2"
)

type RtailDownstreamConfig struct {
	Addr string
}

type RtailDownstream struct {
	addr string
	logs chan []byte
}

func NewRtailDownstream(v interface{}) (Downstream, error) {
	conf, ok := v.(RtailDownstreamConfig)
	if !ok {
		return nil, errkit.Error("Parameter must be RtailDownstreamConfig")
	}

	downstream := &RtailDownstream{
		addr: conf.Addr,
		logs: make(chan []byte),
	}

	go downstream.NewServer()

	return downstream, nil
}

func (d *RtailDownstream) NewServer() {
	server := &http.Server{
		Addr:    d.addr,
		Handler: d.router(),
	}
	http2.ConfigureServer(server, &http2.Server{})
	server.ListenAndServe()
}

func (u *RtailDownstream) router() (router *mux.Router) {
	router = mux.NewRouter()
	router.HandleFunc(
		"/rtail",
		u.tailHandler,
	).Methods("GET")

	return
}

func (d *RtailDownstream) tailHandler(writer http.ResponseWriter, req *http.Request) {
	writer.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(writer, "# ~1KB of junk to force browsers to start rendering immediately: \n")
	io.WriteString(writer, strings.Repeat("# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n", 5))
	io.WriteString(writer, "# xxxxxxxxxxxx BaritoLog Rtail xxxxxxxxxxxx\n")
	io.WriteString(writer, strings.Repeat("# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n", 7))
	writer.(http.Flusher).Flush()

	for log := range d.logs {
		t := time.Now().Format("2006-01-02 15:04:05")
		fmt.Fprintf(writer, "# %s : %s\n", t, string(log))
		writer.(http.Flusher).Flush()
	}
}

func (d *RtailDownstream) Store(timber Timber) (err error) {
	d.logs <- []byte(timber.Message())
	return
}
