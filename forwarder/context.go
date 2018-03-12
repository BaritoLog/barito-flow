package forwarder

import (
	"strings"

	"fmt"
	"log"
	"os"
	"github.com/BaritoLog/go-boilerplate/app"
	"github.com/bsm/sarama-cluster"
)

// Context of forwarder part
type Context interface {
	Init(config app.Configuration) (err error)
	Run() (err error)
}

// receiver implementation
type context struct {
	consumer *cluster.Consumer
}

// NewContext
func NewContext() Context {
	return &context{}
}

// Init
func (c *context) Init(config app.Configuration) (err error) {

	conf := config.(Configuration)
	fmt.Printf("init context\n%+v\n", conf)

	brokers := strings.Split(conf.kafkaBrokers, ",")
	topics := strings.Split(conf.kafkaConsumerTopic, ",")

	c.consumer, err = cluster.NewConsumer(brokers, conf.kafkaConsumerGroupId, topics, c.kafkaConfig())
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

func (c *context) Consume() (err error) {
	consumer := c.consumer

	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
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