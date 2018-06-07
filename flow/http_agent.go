package flow

import (
	"net/http"
	"time"

	"github.com/BaritoLog/go-boilerplate/timekit"
)

type HttpAgent interface {
	Start() error
	Close()
	ServeHTTP(rw http.ResponseWriter, req *http.Request)
}

type httpAgent struct {
	Address string
	Store   func(timber Timber) error
	MaxTps  int
	tps     int
	server  *http.Server
	tick    <-chan time.Time
	stop    chan int
}

func NewHttpAgent(addr string, store func(timber Timber) error, maxTps int) HttpAgent {
	return &httpAgent{
		Address: addr,
		Store:   store,
		MaxTps:  maxTps,
		tps:     maxTps,
	}
}

func (a *httpAgent) Start() error {
	if a.server == nil {
		a.server = &http.Server{
			Addr:    a.Address,
			Handler: a,
		}
	}

	a.tick = time.Tick(timekit.Duration("1s"))
	a.stop = make(chan int)
	a.tps = a.MaxTps

	go a.loopRefillBucket()

	return a.server.ListenAndServe()
}

func (a *httpAgent) Close() {
	if a.server != nil {
		a.server.Close()
	}

	go func() {
		a.stop <- 1
	}()

}

func (a *httpAgent) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if !a.leakBucket() {
		rw.WriteHeader(509)
		rw.Write([]byte("Bandwith Limit Exceeded"))
		return
	}
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
	rw.Write([]byte("OK"))
}

func (a *httpAgent) loopRefillBucket() {
	for {
		select {
		case <-a.tick:
			a.refillBucket()
		case <-a.stop:
			return
		}
	}
}

func (a *httpAgent) refillBucket() {
	a.tps = a.MaxTps
}

func (a *httpAgent) leakBucket() bool {
	if a.tps <= 0 {
		return false
	}

	a.tps--
	return true
}
