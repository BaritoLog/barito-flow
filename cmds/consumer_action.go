package cmds

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/srvkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/BaritoLog/instru"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func ConsumerAction(c *cli.Context) (err error) {

	brokers := configKafkaBrokers()
	groupID := configKafkaGroupId()
	esUrl := configElasticsearchUrl()

	// TODO: get topicSuffix
	service := flow.NewBaritoConsumerService(brokers, groupID, esUrl, "_logs")

	service.Start()
	srvkit.GracefullShutdown(service.Close)

	return

}

func callbackInstrumentation() bool {
	pushMetricUrl := configPushMetricUrl()
	pushMetricToken := configPushMetricToken()
	pushMetricInterval := configPushMetricInterval()

	if pushMetricToken == "" || pushMetricUrl == "" {
		log.Infof("No callback for instrumentation")
		return false
	}

	instru.SetCallback(
		timekit.Duration(pushMetricInterval),
		NewMetricMarketCallback(pushMetricUrl, pushMetricToken),
	)
	return true

}
