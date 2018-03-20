package river

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestElasticsearchDownstream__New_WrongParameter(t *testing.T) {
	_, err := NewElasticsearchDownstream("meh")
	FatalIfWrongError(t, err, "Parameter must be ElasticsearchDownstreamConfig")
}

func TestElasticsearchDownstream_New_NoConnection(t *testing.T) {
	config := ElasticsearchDownstreamConfig{
		Urls: "wrong_url",
	}
	_, err := NewElasticsearchDownstream(config)
	FatalIfWrongError(t, err, "no active connection found: no Elasticsearch node available")

}
