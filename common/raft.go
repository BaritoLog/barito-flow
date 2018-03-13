package common

type Raft interface {
	Start() (err error)
}

func NewRaft(from Upstream, to Downstream) Raft {
	return &raft{
		from: from,
		to:   to,
	}
}

type raft struct {
	from Upstream
	to   Downstream
}

// Drifting
func (r *raft) Start() (err error) {

	go r.from.StartTransport()

	timberCh := r.from.TimberChannel()

	for {
		select {
		case timber := <-timberCh:
			err = r.to.Store(timber)
			if err != nil {
				return
			}
		}

	}

	return
}
