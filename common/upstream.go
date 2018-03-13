package common

import (
	"bufio"
	"io"
	"time"
)

// Upstream
type Upstream interface {
	StartTransport()
	TimberChannel() (timberChannel chan Timber)
}

func NewConsoleUpstream(reader io.Reader) Upstream {
	interval, _ := time.ParseDuration("100ms")
	return &consoleUpstream{
		reader:        bufio.NewReader(reader),
		timberChannel: make(chan Timber),
		interval:      interval,
	}
}
