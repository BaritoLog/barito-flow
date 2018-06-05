package instru

type Exposer interface {
	Expose(Instrumentation) error
	Stop()
}

type dummyExposer struct {
	Err    error
	Instr  Instrumentation
	IsStop bool
}

func (e *dummyExposer) Expose(instr Instrumentation) error {
	e.Instr = instr
	return e.Err
}

func (e *dummyExposer) Stop() {
	e.IsStop = true
}
