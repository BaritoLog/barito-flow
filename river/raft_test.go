package river

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/BaritoLog/go-boilerplate/errkit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestRaft(t *testing.T) {

	buff := &bytes.Buffer{}

	from := NewConsoleUpstream(strings.NewReader("some input\n"))
	to := NewConsoleDownstream(buff)

	raft := NewRaft(from, to)
	raft.Start()

	wait, _ := time.ParseDuration("1ms")
	time.Sleep(wait)

	FatalIf(t, buff.String() != "some input\n", "wrong input")

}

func TestRaft_Drifting_ErrorWhenStore(t *testing.T) {

	from := NewConsoleUpstream(strings.NewReader("some input\n"))
	to := &DummyDownstream{
		ErrStore: errkit.Error("some error"),
	}

	raft := NewRaft(from, to)
	raft.Start()

	wait, _ := time.ParseDuration("1ms")
	time.Sleep(wait)

	err := <-raft.ErrorChannel()
	FatalIfWrongError(t, err, "some error")

}
