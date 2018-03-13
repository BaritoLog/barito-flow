package common

import "io"

// Downstream
type Downstream interface {
	Store(timber Timber) error
}

func NewConsoleDownstream(writer io.Writer) Downstream {
	return &consoleDownstream{
		writer: writer,
	}
}
