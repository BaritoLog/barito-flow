package river

import (
	"testing"
	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestKafkaUpstream_WrongParameter(t *testing.T) {
	_, err := NewKafkaUpstream("meh")
	FatalIfWrongError(t, err, "Parameter must be KafkaUpstreamConfig")
}
