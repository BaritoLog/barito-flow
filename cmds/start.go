package cmds

import (
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

	errCh := raft.ErrorChannel()

	for {
		select {
		case err = <-errCh:
			return err
		}
	}

	return

}
