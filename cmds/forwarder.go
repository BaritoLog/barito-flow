package cmds

import (
	"github.com/BaritoLog/barito-flow/river"
	"github.com/urfave/cli"
	log "github.com/sirupsen/logrus"
)

func Forwarder(c *cli.Context) (err error) {
	conf, err := NewForwarderConfigByEnv()
	if err != nil {
		return
	}
	conf.Info(log.StandardLogger())

	kafka, err := conf.KafkaUpstream()
	if err != nil {
		return
	}

	elasticsearch, err := conf.ElasticsearchDownstream()
	if err != nil {
		return
	}
	raft := river.NewRaft(kafka, elasticsearch)
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
