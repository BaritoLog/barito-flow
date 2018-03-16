package cmds

import (
	"github.com/BaritoLog/barito-flow/river"
	"github.com/urfave/cli"
)

func Rtail(c *cli.Context) (err error) {
	config := StartConfig{
		UpstreamName: "kafka",
		UpStreamConfig: river.KafkaUpstreamConfig{
			Brokers:         []string{"localhost:9092"},
			ConsumerGroupId: "barito-consumer-rtail",
			ConsumerTopic:   []string{"kafka-dummy-topic-test"},
		},
		DownstreamName: "rtail",
		DownstreamConfig: river.RtailDownstreamConfig{
			Addr:	":8081",
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

	raft := river.NewRaft(upstream, downstream)
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
