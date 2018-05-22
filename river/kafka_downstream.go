package river

import (
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
)

type KafkaDownstream struct {
	producer sarama.SyncProducer
	topic    string
}

func NewKafkaDownstream(v interface{}) (Downstream, error) {
	conf, ok := v.(KafkaDownstreamConfig)
	if !ok {
		return nil, errkit.Error("Parameter must be KafkaDownstreamConfig")
	}

	producer, err := sarama.NewSyncProducer(conf.Brokers, conf.SaramaConfig())
	if err != nil {
		return nil, err
	}

	kafkaDs := &KafkaDownstream{
		producer: producer,
		topic:    conf.Topic,
	}

	return kafkaDs, nil
}

func (d *KafkaDownstream) Store(timber Timber) (err error) {
	timber.SetLocation(d.topic)
	message := ConvertToKafkaMessage(timber)
	_, _, err = d.producer.SendMessage(message)
	return
}
