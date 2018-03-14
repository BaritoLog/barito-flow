package cmds

import (
	"github.com/BaritoLog/barito-flow/river"
	"github.com/urfave/cli"
)

func Forwarder(c *cli.Context) (err error) {
	config := StartConfig{
		UpstreamName: "kafka",
		UpStreamConfig: river.KafkaUpstreamConfig{
			Brokers:         []string{"localhost:9092"},
			ConsumerGroupId: "barito-consumer",
			ConsumerTopic:   []string{"kafka-dummy-topic"},
		},
		DownstreamName: "elasticsearch",
		DownstreamConfig: river.ElasticsearchDownstreamConfig{
			Urls:	"http://localhost:9200",
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
