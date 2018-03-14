package river

// Upstream
type Upstream interface {
	StartTransport()
	TimberChannel() chan Timber
	SetErrorChannel(errCh chan error)
	ErrorChannel() chan error
}
