package river

// Upstream
type Upstream interface {
	StartTransport()
	TimberChannel() (timberCh chan Timber)
	SetErrorChannel(errCh chan error)
}
