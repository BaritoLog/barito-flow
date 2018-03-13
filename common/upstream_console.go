package common

import (
	"bufio"
	"time"
)

// ConsoleUpstream
type consoleUpstream struct {
	reader        *bufio.Reader
	timberChannel chan Timber
	interval      time.Duration
}

// Start
func (u *consoleUpstream) StartTransport() {

	for {
		text, _ := u.reader.ReadString('\n')
		u.timberChannel <- Timber(text)
		time.Sleep(u.interval)
	}

}

// Flow
func (u *consoleUpstream) TimberChannel() chan Timber {
	return u.timberChannel
}
