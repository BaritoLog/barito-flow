package cmds

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/srvkit"
	"github.com/Shopify/sarama"
	"github.com/urfave/cli"
)

func ActionBaritoConsumerService(c *cli.Context) (err error) {
	brokers := configKafkaBrokers()
	groupID := configKafkaGroupId()
	esUrl := configElasticsearchUrl()
	topicSuffix := configKafkaTopicSuffix()

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0 // TODO: get version from env

	newTopicEventName := "new_topic_events" // TODO: get from env

	service, err := flow.NewBaritoConsumerService(
		brokers,
		config,
		groupID,
		esUrl,
		topicSuffix,
		newTopicEventName)
	if err != nil {
		return
	}

	service.Start()
	srvkit.GracefullShutdown(service.Close)

	return
}

func ActionBaritoProducerService(c *cli.Context) (err error) {

	address := configProducerAddress()
	kafkaBrokers := configKafkaBrokers()
	maxRetry := configProducerMaxRetry()
	maxTps := configProducerMaxTPS()
	topicSuffix := configKafkaTopicSuffix()

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true
	config.Version = sarama.V0_10_2_1 // TODO: get version from env

	newTopicEventName := "new_topic_events" // TODO: get from env

	srv, err := flow.NewBaritoProducerService(
		address,
		kafkaBrokers,
		config,
		maxTps,
		topicSuffix,
		newTopicEventName)
	if err != nil {
		return
	}
	srvkit.AsyncGracefulShutdown(srv.Close)

	return srv.Start()
}

// TODO: implement on consumer worker
// func callbackInstrumentation() bool {
// 	pushMetricUrl := configPushMetricUrl()
// 	pushMetricToken := configPushMetricToken()
// 	pushMetricInterval := configPushMetricInterval()
//
// 	if pushMetricToken == "" || pushMetricUrl == "" {
// 		log.Infof("No callback for instrumentation")
// 		return false
// 	}
//
// 	instru.SetCallback(
// 		timekit.Duration(pushMetricInterval),
// 		NewMetricMarketCallback(pushMetricUrl, pushMetricToken),
// 	)
// 	return true
//
// }
