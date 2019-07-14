package cmds

import (
	"fmt"
	"time"

	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/srvkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/BaritoLog/instru"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func ActionBaritoConsumerService(c *cli.Context) (err error) {
	if c.Bool("verbose") == true {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	brokers := configKafkaBrokers()
	groupID := configKafkaGroupId()
	esUrl := configElasticsearchUrl()
	topicSuffix := configKafkaTopicSuffix()
	kafkaMaxRetry := configKafkaMaxRetry()
	kafkaRetryInterval := configKafkaRetryInterval()
	newTopicEventName := configNewTopicEvent()
	elasticRetrierInterval := configElasticsearchRetrierInterval()
	esIndexMethod := configEsIndexMethod()
	esBulkSize := configEsBulkSize()
	esFlushIntervalMs := configEsFlushIntervalMs()
	printTPS := configPrintTPS()

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_1 // TODO: get version from env
	if configConsumerRebalancingStrategy() == "RoundRobin" {
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	} else if configConsumerRebalancingStrategy() == "Range" {
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	}

	factory := flow.NewKafkaFactory(brokers, config)

	esConfig := flow.NewEsConfig(
		esIndexMethod,
		esBulkSize,
		time.Duration(esFlushIntervalMs),
		printTPS,
	)

	service := flow.NewBaritoConsumerService(
		factory,
		groupID,
		esUrl,
		topicSuffix,
		kafkaMaxRetry,
		kafkaRetryInterval,
		newTopicEventName,
		elasticRetrierInterval,
		esConfig,
	)

	callbackInstrumentation()

	if err = service.Start(); err != nil {
		return
	}

	srvkit.GracefullShutdown(service.Close)

	return
}

func ActionBaritoProducerService(c *cli.Context) (err error) {
	if c.Bool("verbose") == true {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	address := configProducerAddress()
	kafkaBrokers := configKafkaBrokers()
	maxRetry := configProducerMaxRetry()
	maxTps := configProducerMaxTPS()
	rateLimitResetInterval := configProducerRateLimitResetInterval()
	topicSuffix := configKafkaTopicSuffix()
	kafkaMaxRetry := configKafkaMaxRetry()
	kafkaRetryInterval := configKafkaRetryInterval()
	newTopicEventName := configNewTopicEvent()

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Version = sarama.V0_10_2_1 // TODO: get version from env

	factory := flow.NewKafkaFactory(kafkaBrokers, config)

	srv := flow.NewBaritoProducerService(
		factory,
		address,
		maxTps,
		rateLimitResetInterval,
		topicSuffix,
		kafkaMaxRetry,
		kafkaRetryInterval,
		newTopicEventName)

	err = srv.Start()
	if err != nil {
		return
	}
	srvkit.AsyncGracefulShutdown(srv.Close)

	return
}

func callbackInstrumentation() bool {
	pushMetricUrl := configPushMetricUrl()
	pushMetricInterval := configPushMetricInterval()

	if pushMetricUrl == "" {
		fmt.Print("No callback for instrumentation")
		return false
	}

	instru.SetCallback(
		timekit.Duration(pushMetricInterval),
		NewMetricMarketCallback(pushMetricUrl),
	)
	return true

}
