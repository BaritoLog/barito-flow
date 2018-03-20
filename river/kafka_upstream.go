package river

import (
	"github.com/BaritoLog/go-boilerplate/errkit"
	cluster "github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
)

type kafkaUpstream struct {
	brokers         []string
	consumerGroupId string
	consumerTopic   []string
	consumer        *cluster.Consumer
	timberCh        chan Timber
	errCh           chan error
}

type KafkaUpstreamConfig struct {
	Brokers         []string
	ConsumerGroupId string
	ConsumerTopic   []string
}

func NewKafkaUpstream(v interface{}) (Upstream, error) {
	conf, ok := v.(KafkaUpstreamConfig)
	if !ok {
		return nil, errkit.Error("Parameter must be KafkaUpstreamConfig")
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	consumer, err := cluster.NewConsumer(
		conf.Brokers,
		conf.ConsumerGroupId,
		conf.ConsumerTopic,
		config,
	)
	if err != nil {
		return nil, err
	}

	upstream := &kafkaUpstream{
		consumer: consumer,
		timberCh: make(chan Timber),
		errCh:    make(chan error),
	}
	return upstream, nil
}

func (u *kafkaUpstream) StartTransport() {

	go u.loopError()
	go u.loopNofication()

	u.loopMain()
}

func (u *kafkaUpstream) TimberChannel() chan Timber {
	return u.timberCh

}

func (u *kafkaUpstream) SetErrorChannel(errCh chan error) {
	u.errCh = errCh
}

func (u *kafkaUpstream) ErrorChannel() chan error {
	return u.errCh
}

func (u *kafkaUpstream) loopError() {
	for err := range u.consumer.Errors() {
		u.errCh <- err
	}

}

func (u *kafkaUpstream) loopNofication() {
	for ntf := range u.consumer.Notifications() {
		log.Infof("Rebalanced: %+v\n", ntf)
	}
}

func (u *kafkaUpstream) loopMain() {
	for {
		select {
		case msg, ok := <-u.consumer.Messages():
			if ok {
				log.Infof("Consume Topic:'%s' at Partition:%d Offeet:%d\tKey:'%s'\tValue:%s\n",
					msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				u.timberCh <- Timber{
					Location: u.consumerTopic[0],
					Data:     msg.Value,
				}
				u.consumer.MarkOffset(msg, "") // mark message as processed
			}
		}
	}
}
