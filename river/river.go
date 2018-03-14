package river

// Raft
type Raft interface {
	Start()
	ErrorChannel() (errCh chan error)
}

// Downstream
type Downstream interface {
	Store(timber Timber) error
}

// Upstream
type Upstream interface {
	StartTransport()
	TimberChannel() chan Timber
	SetErrorChannel(errCh chan error)
	ErrorChannel() chan error
}

// Timber
type Timber struct {
	Location string
	Data     []byte
}
