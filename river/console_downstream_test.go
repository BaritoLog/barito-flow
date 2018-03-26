package river

import (
	"bytes"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestConsoleDownstream(t *testing.T) {

	timber := Timber{
		Location: "some location",
		Message:  "some data",
	}

	buff := bytes.Buffer{}
	downstream := NewConsoleDownstream(&buff)
	err := downstream.Store(timber)
	FatalIfError(t, err)

	get := buff.String()
	FatalIf(t, get != "some location||some data\n", get)

}
