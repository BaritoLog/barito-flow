package cmds

import (
	"github.com/BaritoLog/barito-flow/river"
	"github.com/urfave/cli"
)

func Start(c *cli.Context) (err error) {
	config := StartConfig{
		UpstreamName:   "stdin",
		DownstreamName: "kafka",
		DownstreamConfig: river.KafkaDownstreamConfig{
			Brokers:          []string{"localhost:9092"},
			ProducerRetryMax: 10,
		},
	}

	upstream, err := config.Upstream()
	if err != nil {
		return
	}

	downstream, err := config.Downstream()
	if err != nil {
		return
	}

	transporter := river.NewTransporter(upstream, downstream)
	transporter.Start()

	errCh := transporter.ErrorChannel()

	for {
		select {
		case err = <-errCh:
			return err
		}
	}

	return

}
