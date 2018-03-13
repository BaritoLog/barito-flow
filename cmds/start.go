package cmds

import (
	"runtime"

	"github.com/BaritoLog/barito-flow/river"
	"github.com/urfave/cli"
)

func Start(c *cli.Context) (err error) {
	config := StartConfig{
		UpstreamName:   "stdin",
		DownstreamName: "stdout",
	}

	raft := river.NewRaft(
		config.Upstream(),
		config.Downstream(),
	)
	raft.Start()

	for {
		runtime.Gosched()
	}

	return

}
