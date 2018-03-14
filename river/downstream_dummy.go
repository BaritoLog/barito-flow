package river

type DummyDownstream struct {
	ErrStore error
}

func (d *DummyDownstream) Store(timber Timber) (err error) {
	return d.ErrStore
}
