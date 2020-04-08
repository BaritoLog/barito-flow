package cmds

import (
	"fmt"
	"time"

	"github.com/BaritoLog/barito-flow/prome"

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

	prome.InitConsumerInstrumentation()

	brokers := configKafkaBrokers()
	groupID := configKafkaGroupId()
	esUrls := configElasticsearchUrls()
	topicSuffix := configKafkaTopicSuffix()
	kafkaMaxRetry := configKafkaMaxRetry()
	kafkaRetryInterval := configKafkaRetryInterval()
	newTopicEventName := configNewTopicEvent()
	elasticRetrierInterval := configElasticsearchRetrierInterval()
	esIndexMethod := configEsIndexMethod()
	esBulkSize := configEsBulkSize()
	esFlushIntervalMs := configEsFlushIntervalMs()
	printTPS := configPrintTPS()
	elasticUsername := configElasticUsername()
	elasticPassword := configElasticPassword()

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

	consumerParams := map[string]interface{}{
		"factory":                factory,
		"groupID":                groupID,
		"elasticUrls":            esUrls,
		"topicSuffix":            topicSuffix,
		"kafkaMaxRetry":          kafkaMaxRetry,
		"kafkaRetryInterval":     kafkaRetryInterval,
		"newTopicEventName":      newTopicEventName,
		"elasticRetrierInterval": elasticRetrierInterval,
		"esConfig":               esConfig,
		"elasticUsername":        elasticUsername,
		"elasticPassword":        elasticPassword,
	}

	service := flow.NewBaritoConsumerService(consumerParams)

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

	prome.InitProducerInstrumentation()

	grpcAddr := configProducerAddressGrpc()
	restAddr := configProducerAddressRest()
	kafkaBrokers := configKafkaBrokers()
	maxRetry := configProducerMaxRetry()
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

	producerParams := map[string]interface{}{
		"factory":                factory,
		"grpcAddr":               grpcAddr,
		"restAddr":               restAddr,
		"rateLimitResetInterval": rateLimitResetInterval,
		"topicSuffix":            topicSuffix,
		"kafkaMaxRetry":          kafkaMaxRetry,
		"kafkaRetryInterval":     kafkaRetryInterval,
		"newEventTopic":          newTopicEventName,
	}

	service := flow.NewProducerService(producerParams)

	go service.Start()
	if configServeRestApi() {
		go service.LaunchREST()
	}

	srvkit.GracefullShutdown(service.Close)
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
