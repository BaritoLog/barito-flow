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
	producerMaxTps := configProducerMaxTPS()
	topicSuffix := configKafkaTopicSuffix()

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true

	// kafka producer
	// TODO: move producer to BaritoProducerService
	kafkaProducer, err := sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		return
	}

	srv := flow.NewBaritoProducerService(address, kafkaProducer, producerMaxTps, topicSuffix)
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
