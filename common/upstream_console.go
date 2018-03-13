package common

import (
	"bufio"
	"time"
)

// ConsoleUpstream
type consoleUpstream struct {
	reader   *bufio.Reader
	timberCh chan Timber
	errCh    chan error
	interval time.Duration
}

// Start
func (u *consoleUpstream) StartTransport() {
	for {
		text, _ := u.reader.ReadString('\n')
		u.timberCh <- Timber(text)
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
