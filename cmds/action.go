package cmds

import (
	"context"
	"fmt"
	"time"

	"github.com/BaritoLog/barito-flow/prome"

	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/srvkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/BaritoLog/instru"
	"github.com/Shopify/sarama"
	"github.com/mailgun/gubernator/v2"
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
	elasticRetrierMaxRetry := configElasticsearchRetrierMaxRetry()
	esIndexMethod := configEsIndexMethod()
	esBulkSize := configEsBulkSize()
	esFlushIntervalMs := configEsFlushIntervalMs()
	printTPS := configPrintTPS()
	elasticUsername := configElasticUsername()
	elasticPassword := configElasticPassword()

	config := sarama.NewConfig()
	config.Consumer.Offsets.CommitInterval = time.Second
	config.Version = sarama.V2_6_0_0 // TODO: get version from env
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
		"elasticRetrierMaxRetry": elasticRetrierMaxRetry,
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
	ignoreKafkaOptions := configProducerIgnoreKafkaOptions()
	topicSuffix := configKafkaTopicSuffix()
	kafkaMaxRetry := configKafkaMaxRetry()
	kafkaRetryInterval := configKafkaRetryInterval()
	newTopicEventName := configNewTopicEvent()
	grpcMaxRecvMsgSize := configGrpcMaxRecvMsgSize()
	rateLimiterOpt := configRateLimiterOpt()

	if rateLimiterOpt == RateLimiterOptUndefined {
		return fmt.Errorf("undefined rate limiter options, allowed options are %v", RateLimiterAllowedOpts)
	}

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = 1
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionZSTD
	config.Producer.CompressionLevel = sarama.CompressionLevelDefault
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Producer.Flush.Bytes = 16000
	config.Producer.Flush.Frequency = 100 * time.Millisecond
	config.Version = sarama.V2_6_0_0 // TODO: get version from env

	// gubernator

	factory := flow.NewKafkaFactory(kafkaBrokers, config)

	var rateLimiter flow.RateLimiter

	switch rateLimiterOpt {
	case RateLimiterOptRedis:
		// ToDo add some redis here
	case RateLimiterOptGubernator:
		rateLimiter, err = setupGubernatorRateLimiter(context.Background(), rateLimitResetInterval)
		if err != nil {
			return fmt.Errorf("failed to setup gubernator rate limiter. %w", err)
		}
	case RateLimiterOptLocal:
		rateLimiter = flow.NewRateLimiter(rateLimitResetInterval)
	}

	producerParams := map[string]interface{}{
		"factory":            factory,
		"grpcAddr":           grpcAddr,
		"restAddr":           restAddr,
		"topicSuffix":        topicSuffix,
		"kafkaMaxRetry":      kafkaMaxRetry,
		"kafkaRetryInterval": kafkaRetryInterval,
		"newEventTopic":      newTopicEventName,
		"grpcMaxRecvMsgSize": grpcMaxRecvMsgSize,
		"ignoreKafkaOptions": ignoreKafkaOptions,
		"limiter":            rateLimiter,
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

func setupGubernatorRateLimiter(ctx context.Context, rateLimitResetInterval int) (*flow.GubernatorRateLimiter, error) {
	conf, err := gubernator.SetupDaemonConfig(log.StandardLogger(), "")
	if err != nil {
		return nil, fmt.Errorf("failed to initiate gubernator config. %w", err)
	}
	daemon, err := gubernator.SpawnDaemon(ctx, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate gubernator. %w", err)
	}

	return flow.NewGubernatorRateLimiter(daemon.V1Server, rateLimitResetInterval), nil
}
