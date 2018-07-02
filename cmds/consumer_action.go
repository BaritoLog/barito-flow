package cmds

import (
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

	log.Infof("[Start Consumer]")

	brokers := getKafkaBrokers()
	groupID := getKafkaGroupId()
	topics := getKafkaConsumerTopics()
	esUrl := getElasticsearchUrl()

	//

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

	// consumer config
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// kafka consumer
	consumer, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		return
	}

	worker := flow.NewConsumerWorker(consumer, client)
	worker.OnError(func(err error) {
		log.Warn(err.Error())
	})

	srvkit.AsyncGracefulShutdown(worker.Close)

	return worker.Start()

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
		NewMetricMarketCallback(pushMetricUrl, pushMetricToken),
	)
	return true

}
