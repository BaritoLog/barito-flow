package river

import (
	"fmt"
	"io"
)

// ConsoleDownstream
type consoleDownstream struct {
	writer io.Writer
}

// NewConsoleDownstream
func NewConsoleDownstream(writer io.Writer) Downstream {
	return &consoleDownstream{
		writer: writer,
	}
}

// Store
func (d consoleDownstream) Store(timber Timber) (err error) {
	fmt.Fprintf(d.writer, "%s||%s\n", timber.Location(), timber.Message())
	return
}
