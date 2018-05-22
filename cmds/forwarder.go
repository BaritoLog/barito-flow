package cmds

import (
	"github.com/BaritoLog/barito-flow/river"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
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
	transporter := river.NewTransporter(kafka, elasticsearch)
	transporter.Start()

	errCh := transporter.ErrorChannel()

	for {
		select {
		case err = <-errCh:
			log.Error(err)
		}
	}

	return

}
