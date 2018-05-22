package river

import (
	"net/http"

	"github.com/BaritoLog/go-boilerplate/errkit"
)

type receiverUpstream struct {
	addr      string
	appSecret string
	timberCh  chan Timber
	errCh     chan error
}

type ReceiverUpstreamConfig struct {
	Addr      string
	AppSecret string
}

func NewReceiverUpstream(v interface{}) (Upstream, error) {
	conf, ok := v.(ReceiverUpstreamConfig)
	if !ok {
		return nil, errkit.Error("Parameter must be ReceiverUpstreamConfig")
	}

	upstream := &receiverUpstream{
		addr:      conf.Addr,
		appSecret: conf.AppSecret,
		timberCh:  make(chan Timber),
		errCh:     make(chan error),
	}
	return upstream, nil
}

func (u *receiverUpstream) StartTransport() {
	server := &http.Server{
		Addr:    u.addr,
		Handler: u,
	}

	u.errCh <- server.ListenAndServe()
}

func (u *receiverUpstream) TimberChannel() chan Timber {
	return u.timberCh

}
func (u *receiverUpstream) SetErrorChannel(errCh chan error) {
	u.errCh = errCh
}

func (u *receiverUpstream) ErrorChannel() chan error {
	return u.errCh
}

func (u *receiverUpstream) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	timber := NewTimberFromRequest(req)
	go u.SendTimber(timber)

	writer.WriteHeader(http.StatusOK)
}

func (u *receiverUpstream) SendTimber(timber Timber) {
	u.timberCh <- timber
}
