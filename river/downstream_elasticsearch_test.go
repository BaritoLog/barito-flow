package river

import (
	"testing"
	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestElasticsearchDownstream__WrongParameter(t *testing.T) {
	_, err := NewElasticsearchDownstream("meh")
	FatalIfWrongError(t, err, "Parameter must be ElasticsearchDownstreamConfig")
}
