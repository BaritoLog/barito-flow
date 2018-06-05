package instru

type Callback interface {
	OnCallback(inst Instrumentation) error
}

type dummyCallback struct {
	Instr Instrumentation
	Err   error
}

func (c *dummyCallback) OnCallback(instr Instrumentation) error {
	c.Instr = instr
	return c.Err
}
