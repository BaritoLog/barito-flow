package common

type Raft interface {
	Start()
	ErrorChannel() (errCh chan error)
}

func NewRaft(from Upstream, to Downstream) Raft {
	return &raft{
		from:  from,
		to:    to,
		errCh: make(chan error),
	}
}

type raft struct {
	from  Upstream
	to    Downstream
	errCh chan error
}

// Drifting
func (r *raft) Start() {

	go r.from.StartTransport()
	go r.start()

}

func (r *raft) start() {
	timberCh := r.from.TimberChannel()
	for {
		select {
		case timber := <-timberCh:
			err := r.to.Store(timber)
			if err != nil {
				r.errCh <- err
			}
		}
	}
}

func (r *raft) ErrorChannel() (errCh chan error) {
	return r.errCh
}
