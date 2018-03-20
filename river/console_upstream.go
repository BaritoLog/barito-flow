package river

import (
	"bufio"
	"io"
	"strings"
	"time"
)

// ConsoleUpstream
type consoleUpstream struct {
	reader   *bufio.Reader
	timberCh chan Timber
	errCh    chan error
	interval time.Duration
}

func NewConsoleUpstream(reader io.Reader) Upstream {
	interval, _ := time.ParseDuration("100ms")
	return &consoleUpstream{
		reader:   bufio.NewReader(reader),
		timberCh: make(chan Timber),
		interval: interval,
	}
}

// Start
func (u *consoleUpstream) StartTransport() {
	for {
		text, _ := u.reader.ReadString('\n')
		if len(text) < 1 {
			continue
		}
		text = text[:len(text)-1]
		chunks := strings.Split(text, "||")

		location := chunks[0]
		data := ""
		if len(chunks) > 1 {
			data = chunks[1]
		}

		u.timberCh <- Timber{
			Location: location,
			Data:     []byte(data),
		}
		time.Sleep(u.interval)
	}
}

// Flow
func (u *consoleUpstream) TimberChannel() chan Timber {
	return u.timberCh
}

func (u *consoleUpstream) SetErrorChannel(errCh chan error) {
	u.errCh = errCh
}

func (u *consoleUpstream) ErrorChannel() chan error {
	return u.errCh
}
