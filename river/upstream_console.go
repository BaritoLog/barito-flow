package river

import (
	"bufio"
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

// Start
func (u *consoleUpstream) StartTransport() {
	for {
		text, _ := u.reader.ReadString('\n')
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
