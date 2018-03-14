package river

import (
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
)

type kafkaUpstream struct {
	brokers         []string
	consumerGroupId string
	consumerTopic   []string
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

	upstream := &kafkaUpstream{
		brokers:         conf.Brokers,
		consumerGroupId: conf.ConsumerGroupId,
		consumerTopic:   conf.ConsumerTopic,
		timberCh:             make(chan Timber),
		errCh:                make(chan error),
	}
	return upstream, nil
}

func (u *kafkaUpstream) StartTransport() {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	consumer, err := cluster.NewConsumer(u.brokers, u.consumerGroupId, u.consumerTopic, config)

	go func() {
		for err := range consumer.Errors() {
			u.errCh <- err
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
				u.Consume(msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		}
	}

	u.errCh <- err
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

func (u *kafkaUpstream) Consume(log []byte) {
	timber := Timber{
		Location: u.consumerTopic[0],
		Data:     log,
	}
	u.timberCh <- timber
}
