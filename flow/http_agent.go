package flow

import (
	"net/http"
)

type HttpAgent struct {
	Address string
	Store   func(timber Timber) error
	server  *http.Server
}

func (a *HttpAgent) Start() error {
	if a.server == nil {
		a.server = &http.Server{
			Addr:    a.Address,
			Handler: a,
		}
	}

	return a.server.ListenAndServe()
}

func (a *HttpAgent) Close() {
	if a.server != nil {
		a.server.Close()
	}
}

func (a *HttpAgent) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	timber := NewTimberFromRequest(req)
	if a.Store != nil {
		err := a.Store(timber)
		if err != nil {
			rw.WriteHeader(http.StatusBadGateway)
			rw.Write([]byte(err.Error()))
			return
		}
	}

	rw.WriteHeader(http.StatusOK)
}
