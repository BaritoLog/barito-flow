package cmds

import (
	"fmt"

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

	// TODO: get topicSuffix
	service := flow.NewBaritoConsumerService(brokers, groupID, esUrl, topicSuffix)

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

	newEventTopic := "new_app_events"
	fmt.Println(newEventTopic)

	srv, err := flow.NewBaritoProducerService(
		address,
		kafkaBrokers,
		config,
		maxTps,
		topicSuffix,
		newEventTopic)
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
