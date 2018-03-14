package cmds

import (
	"fmt"
	"os"

	"github.com/BaritoLog/barito-flow/river"
)

type StartConfig struct {
	UpstreamName     string      `json:"upstream_name"`
	UpStreamConfig   interface{} `json:"upstream_config"`
	DownstreamName   string      `json:"downstream_name"`
	DownstreamConfig interface{} `json:"downstream_config"`
}

func (c StartConfig) Upstream() (upstream river.Upstream, err error) {
	switch c.UpstreamName {
	case "stdin":
		upstream = river.NewConsoleUpstream(os.Stdin)
		return
	case "kafka":
		upstream, err = river.NewKafkaUpstream(c.UpStreamConfig)
		return
	case "receiver":
		upstream, err = river.NewReceiverUpstream(c.UpStreamConfig)
		return
	}

	err = fmt.Errorf(c.UpstreamName)
	return
}

func (c StartConfig) Downstream() (downstream river.Downstream, err error) {
	switch c.DownstreamName {
	case "stdout":
		downstream = river.NewConsoleDownstream(os.Stdout)
		return
	case "kafka":
		downstream, err = river.NewKafkaDownstream(c.DownstreamConfig)
		return
	case "elasticsearch":
		downstream, err = river.NewElasticsearchDownstream(c.DownstreamConfig)
		return
	}

	err = fmt.Errorf("can't find downstream with name '%s'", c.DownstreamName)
	return
}
