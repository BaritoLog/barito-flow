package common

import (
	"fmt"
	"io"
)

// ConsoleDownstream
type consoleDownstream struct {
	writer io.Writer
}

// Store
func (d consoleDownstream) Store(timber Timber) (err error) {
	fmt.Fprintf(d.writer, "%s", timber.Data)
	return
}
