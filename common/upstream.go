package common

import (
	"bufio"
	"io"
	"time"
)

// Upstream
type Upstream interface {
	StartTransport()
	TimberChannel() (timberCh chan Timber)
	SetErrorChannel(errCh chan error)
}

func NewConsoleUpstream(reader io.Reader) Upstream {
	interval, _ := time.ParseDuration("100ms")
	return &consoleUpstream{
		reader:   bufio.NewReader(reader),
		timberCh: make(chan Timber),
		interval: interval,
	}
}
