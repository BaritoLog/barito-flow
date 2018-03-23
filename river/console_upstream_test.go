package river

import (
	"strings"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestConsoleUpstream(t *testing.T) {

	upstream := NewConsoleUpstream(strings.NewReader("some location||some input\n"))
	go upstream.StartTransport()

	timekit.Sleep("1ms")

	timber := <-upstream.TimberChannel()
	loc := timber.Location
	data := string(timber.Message)

	FatalIf(t, loc != "some location", "wrong location: %s", loc)
	FatalIf(t, data != "some input", "wrong data: %s", data)

}

func TestConsoleUpstream_SetErrorChannel(t *testing.T) {
	errCh := make(chan error)
	console := consoleUpstream{}

	console.SetErrorChannel(errCh)
	FatalIf(t, errCh != console.ErrorChannel(), "SetErrorChannel is not working")

}
