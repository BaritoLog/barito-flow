package river

// Downstream
type Downstream interface {
	Store(timber Timber) error
}

type DummyDownstream struct {
	ErrStore error
}

func (d *DummyDownstream) Store(timber Timber) (err error) {
	return d.ErrStore
}
