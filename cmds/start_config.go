package cmds

import (
	"os"

	"github.com/BaritoLog/barito-flow/river"
)

type StartConfig struct {
	UpstreamName     string      `json:"upstream_name"`
	UpStreamConfig   interface{} `json:"upstream_config"`
	DownstreamName   string      `json:"downstream_name"`
	DownstreamConfig interface{} `json:"downstream_config"`
}

func (c StartConfig) Upstream() river.Upstream {
	switch c.UpstreamName {
	case "stdin":
		return river.NewConsoleUpstream(os.Stdin)
	}
	return nil
}

func (c StartConfig) Downstream() river.Downstream {
	switch c.DownstreamName {
	case "stdout":
		return river.NewConsoleDownstream(os.Stdout)
	}
	return nil
}
