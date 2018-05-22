package river

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/BaritoLog/go-boilerplate/errkit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestRaft(t *testing.T) {
	buff := &bytes.Buffer{}

	from := NewConsoleUpstream(strings.NewReader("some location||some input\n"))
	to := NewConsoleDownstream(buff)

	transporter := NewTransporter(from, to)
	transporter.Start()

	timekit.Sleep("1ms")

	FatalIf(t, buff.String() != "some location||some input\n", "wrong input")
}

func TestRaft_Drifting_ErrorWhenStore(t *testing.T) {

	from := NewConsoleUpstream(strings.NewReader("some location||some input"))
	to := &DummyDownstream{
		ErrStore: errkit.Error("some error"),
	}

	transporter := NewTransporter(from, to)
	transporter.Start()

	wait, _ := time.ParseDuration("1ms")
	time.Sleep(wait)

	err := <-transporter.ErrorChannel()
	FatalIfWrongError(t, err, "Error when store timber: some error")

}
