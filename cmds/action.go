package cmds

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/BaritoLog/barito-flow/prome"
	"github.com/BaritoLog/barito-flow/redact"

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
	uniqueGroupID := configUniqueGroupID()
	esUrls := configElasticsearchUrls()
	topicPrefix := configKafkaTopicPrefix()
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
	config.Consumer.Group.Session.Timeout = time.Duration(configConsumerGroupSessionTimeout()) * time.Second
	config.Consumer.Group.Heartbeat.Interval = time.Duration(configConsumerGroupHeartbeatInterval()) * time.Second
	config.Consumer.MaxProcessingTime = time.Duration(configConsumerMaxProcessingTime()) * time.Millisecond
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
		"uniqueGroupID":          uniqueGroupID,
		"elasticUrls":            esUrls,
		"topicPrefix":            topicPrefix,
		"topicSuffix":            topicSuffix,
		"kafkaMaxRetry":          kafkaMaxRetry,
		"kafkaRetryInterval":     kafkaRetryInterval,
		"newTopicEventName":      newTopicEventName,
		"elasticRetrierInterval": elasticRetrierInterval,
		"elasticRetrierMaxRetry": elasticRetrierMaxRetry,
		"esConfig":               esConfig,
		"elasticUsername":        elasticUsername,
		"elasticPassword":        elasticPassword,
		"redactor":               setupRedactor(),
	}

	// if elasticsearch using mTLS
	if elasticCaCrt := configElasticCaCrt(); elasticCaCrt != "" {
		consumerParams["elasticCaCrt"] = elasticCaCrt
		consumerParams["elasticClientCrt"] = configElasticClientCrt()
		consumerParams["elasticClientKey"] = configElasticClientKey()
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
	topicPrefix := configKafkaTopicPrefix()
	topicSuffix := configKafkaTopicSuffix()
	kafkaMaxRetry := configKafkaMaxRetry()
	kafkaRetryInterval := configKafkaRetryInterval()
	newTopicEventName := configNewTopicEvent()
	grpcMaxRecvMsgSize := configGrpcMaxRecvMsgSize()
	rateLimiterOpt := configRateLimiterOpt()
	maxMessageBytes := configProducerMaxMessageBytes()

	if rateLimiterOpt == RateLimiterOptUndefined {
		return fmt.Errorf("undefined rate limiter options, allowed options are %v", RateLimiterAllowedOpts)
	}

	redisUrl := configRedisUrl()
	redisPassword := configRedisPassword()
	redisKeyPrefix := configRedisKeyPrefix()

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = 1
	config.Producer.MaxMessageBytes = maxMessageBytes
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
		rateLimiter, err = setupRedisRateLimiter(context.Background(),
			redisUrl, redisPassword, redisKeyPrefix, rateLimitResetInterval)
		if err != nil {
			return fmt.Errorf("failed to setup redis rate limiter. %w", err)
		}
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
		"topicPrefix":        topicPrefix,
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

func ActionBaritoConsumerGCSService(c *cli.Context) (err error) {
	log.SetLevel(log.DebugLevel)

	prome.InitConsumerInstrumentation()
	prome.InitGCSConsumerInstrumentation()

	brokers := configKafkaBrokers()

	config := sarama.NewConfig()
	// we want to use manual commit
	// so use high interval to avoid auto commit
	config.Consumer.Offsets.CommitInterval = 999999 * time.Hour
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Version = sarama.V2_6_0_0 // TODO: get version from env
	config.Consumer.Group.Session.Timeout = time.Duration(configConsumerGroupSessionTimeout()) * time.Second
	config.Consumer.Group.Heartbeat.Interval = time.Duration(configConsumerGroupHeartbeatInterval()) * time.Second
	config.Consumer.MaxProcessingTime = time.Duration(configConsumerMaxProcessingTime()) * time.Millisecond
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	kafkaFactory := flow.NewKafkaFactory(brokers, config)
	consumerOutputFactory := flow.NewConsumerOutputFactory()

	service := flow.NewBaritoKafkaConsumerGCSFromEnv(kafkaFactory, consumerOutputFactory)

	callbackInstrumentation()

	if err = service.Start(); err != nil {
		return
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

	return flow.NewGubernatorRateLimiter(daemon, rateLimitResetInterval), nil
}

func setupRedisRateLimiter(_ context.Context,
	redisUrl, redisPassword, redisKeyPrefix string, rateLimitResetInterval int) (*flow.RedisRateLimiter, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisUrl,
		Password: redisPassword,
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping redis client. %w", err)
	}

	return flow.NewRedisRateLimiter(redisClient,
		flow.WithDuration(time.Duration(rateLimitResetInterval)*time.Second),
		flow.WithKeyPrefix(redisKeyPrefix),
		flow.WithFallbackToLocal(flow.NewRateLimiter(rateLimitResetInterval)),
		flow.WithMutex(),
	), nil
}

func setupRedactor() *redact.Redactor {
	var redactor *redact.Redactor
	var err error
	if redactorRulesMap := configRedactorRulesMap(); redactorRulesMap != "" {
		redactor, err = redact.NewRedactorFromJSON(redactorRulesMap)
		if err != nil {
			return nil
		}
	}
	return redactor
}
