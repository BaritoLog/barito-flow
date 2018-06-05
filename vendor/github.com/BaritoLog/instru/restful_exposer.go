package instru

import (
	"encoding/json"
	"net/http"
)

type restfulExposer struct {
	Addr   string
	instr  Instrumentation
	server *http.Server
}

func NewRestfulExposer(addr string) Exposer {
	return &restfulExposer{Addr: addr}
}

func (e *restfulExposer) Expose(instr Instrumentation) error {
	if e.server == nil {
		e.server = &http.Server{
			Addr:    e.Addr,
			Handler: e,
		}
	}

	e.instr = instr

	return e.server.ListenAndServe()
}

func (e *restfulExposer) Stop() {
	if e.server != nil {
		e.server.Close()
	}
}

func (e *restfulExposer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	data, _ := json.Marshal(e.instr)
	rw.Write(data)
}
