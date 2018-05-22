package river

import "github.com/BaritoLog/go-boilerplate/errkit"

const (
	StoreError = errkit.Error("Error when store timber")
)

// Raft
type Transporter interface {
	Start()
	ErrorChannel() (errCh chan error)
}

type transporter struct {
	from  Upstream
	to    Downstream
	errCh chan error
}

func NewTransporter(from Upstream, to Downstream) Transporter {
	return &transporter{
		from:  from,
		to:    to,
		errCh: make(chan error),
	}
}

// Drifting
func (r *transporter) Start() {
	r.from.SetErrorChannel(r.errCh)

	go r.from.StartTransport()
	go r.start()

}

func (r *transporter) start() {
	timberCh := r.from.TimberChannel()
	for {
		select {
		case timber := <-timberCh:
			err := r.to.Store(timber)
			if err != nil {
				r.errCh <- errkit.Concat(StoreError, err)
			}
		}
	}
}

func (r *transporter) ErrorChannel() (errCh chan error) {
	return r.errCh
}
