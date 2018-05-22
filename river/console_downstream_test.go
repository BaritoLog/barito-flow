package river

import (
	"bytes"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestConsoleDownstream(t *testing.T) {

	timber := Timber{}
	timber.SetLocation("some location")
	timber.SetMessage("some data")

	buff := bytes.Buffer{}
	downstream := NewConsoleDownstream(&buff)
	err := downstream.Store(timber)
	FatalIfError(t, err)

	get := buff.String()
	FatalIf(t, get != "some location||some data\n", get)

}
