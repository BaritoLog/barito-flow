package river

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
