package forwarder

import (
	"strings"

	ctx "context"
	"fmt"
	"github.com/BaritoLog/go-boilerplate/app"
	"github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v6"
	"encoding/json"
	"time"
)

// Context of forwarder part
type Context interface {
	Init(config app.Configuration) (err error)
	Run() (err error)
}

// receiver implementation
type context struct {
	consumer *cluster.Consumer
	upstream *elastic.Client
	configuration Configuration
}

var indexMapping string = `{
  "template" : "baritolog-*",
  "version" : 60001,
  "settings" : {
    "index.refresh_interval" : "5s"
  },
  "mappings" : {
    "_default_" : {
      "dynamic_templates" : [ {
        "message_field" : {
          "path_match" : "message",
          "match_mapping_type" : "string",
          "mapping" : {
            "type" : "text",
            "norms" : false
          }
        }
      }, {
        "string_fields" : {
          "match" : "*",
          "match_mapping_type" : "string",
          "mapping" : {
            "type" : "text", "norms" : false,
            "fields" : {
              "keyword" : { "type": "keyword", "ignore_above": 256 }
            }
          }
        }
      } ],
      "properties" : {
        "@timestamp": { "type": "date"},
        "@version": { "type": "keyword"}
      }
    }
  }
}`

// NewContext
func NewContext() Context {
	return &context{}
}

// Init
func (c *context) Init(config app.Configuration) (err error) {

	conf := config.(Configuration)
	c.configuration = conf
	fmt.Printf("init context\n%+v\n", conf)

	brokers := strings.Split(conf.kafkaBrokers, ",")
	topics := strings.Split(conf.kafkaConsumerTopic, ",")

	c.consumer, err = cluster.NewConsumer(brokers, conf.kafkaConsumerGroupId, topics, c.kafkaConfig())
	if err != nil {
		return
	}

	c.upstream, err = elastic.NewClient(elastic.SetURL(conf.elasticsearchUrls))
	if err != nil {
		return
	}

	return
}

// Run
func (c *context) Run() (err error) {
	err = c.Consume()
	return
}

func (c *context) Background() ctx.Context {
	return ctx.Background()
}

func (c *context) Consume() (err error) {
	consumer := c.consumer

	go func() {
		for err := range consumer.Errors() {
			log.Fatalf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for ntf := range consumer.Notifications() {
			log.Infof("Rebalanced: %+v\n", ntf)
		}
	}()

	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				log.Infof("%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				c.Stream(msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		}
	}
}

// kafka cluster config
func (c *context) kafkaConfig() (config *cluster.Config) {
	config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	return
}

func (c *context) Stream(message []byte) (err error) {
	var f,g interface{}
	err = json.Unmarshal(message, &f)
	if err != nil {
		return
	}
	m := f.(map[string]interface{})

	indexName := fmt.Sprintf("%s-%s", c.configuration.elasticsearchIndexPrefix,time.Now().Format("2006.01.02"))

	exists, err := c.upstream.IndexExists(indexName).Do(c.Background())
	if err != nil {
		return
	}

	err = json.Unmarshal([]byte(indexMapping), &g)
	mapping := g.(map[string]interface{})

	if !exists {
		_, err := c.upstream.CreateIndex(indexName).BodyJson(mapping).Do(c.Background())
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	_, err = c.upstream.Index().
		Index(indexName).
		Type("fluentd").
		BodyJson(m).
		Do(c.Background())
	if err != nil {
		return
	}

	return
}