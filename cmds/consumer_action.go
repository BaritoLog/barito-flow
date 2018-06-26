package cmds

import (
	"fmt"

	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/srvkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/BaritoLog/instru"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func ConsumerAction(c *cli.Context) (err error) {

	brokers := getKafkaBrokers()
	groupID := getKafkaGroupId()
	topics := getKafkaConsumerTopics()
	esUrl := getElasticsearchUrl()

	log.Infof("[Start Consumer]")
	log.Infof("KafkaBrokers: %v", EnvKafkaBrokers, brokers)
	log.Infof("KafkaGroupID: %s", EnvKafkaGroupID, groupID)
	log.Infof("KafkaConsumerTopics:%v", EnvKafkaConsumerTopics, topics)
	log.Infof("ElasticsearchUrl:%v", EnvElasticsearchUrl, esUrl)

	callbackInstrumentation()

	// elastic client
	client, err := elastic.NewClient(
		elastic.SetURL(esUrl),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)

	storeman := flow.NewElasticStoreman(client)

	// consumer config
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// kafka consumer
	consumer, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		return
	}

	agent := flow.KafkaAgent{
		Consumer: consumer,
		Store:    storeman.Store,
		OnError: func(err error) {
			fmt.Println(err.Error())
		},
	}

	srvkit.AsyncGracefulShutdown(agent.Close)

	return agent.Start()

}

func callbackInstrumentation() bool {
	pushMetricUrl := getPushMetricUrl()
	pushMetricToken := getPushMetricToken()
	pushMetricInterval := getPushMetricInterval()

	if pushMetricToken == "" || pushMetricUrl == "" {
		log.Infof("No callback for instrumentation")
		return false
	}

	log.Infof("PushMetricUrl: %v", EnvPushMetricUrl, pushMetricUrl)
	log.Infof("PushMetricToken: %v", EnvPushMetricToken, pushMetricToken)
	log.Infof("PushMetricInterval: %v", EnvPushMetricInterval, pushMetricInterval)
	instru.SetCallback(
		timekit.Duration(pushMetricInterval),
		flow.NewMetricMarketCallback(pushMetricUrl, pushMetricToken),
	)
	return true

}
