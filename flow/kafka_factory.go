package flow

import (
	"context"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/IBM/sarama"
)

type kafkaFactory struct {
	config  *sarama.Config
	brokers []string
}

func NewKafkaFactory(brokers []string, config *sarama.Config) types.KafkaFactory {
	return &kafkaFactory{
		brokers: brokers,
		config:  config,
	}
}

func (f kafkaFactory) MakeKafkaAdmin() (admin types.KafkaAdmin, err error) {
	client, err := sarama.NewClient(f.brokers, f.config)
	if err != nil {
		return nil, err
	}

	admin, err = NewKafkaAdmin(client)
	if err != nil {
		return nil, err
	}

	return
}

func (f kafkaFactory) MakeClusterConsumer(groupID, topic string, initialOffset int64) (consumer types.ClusterConsumer, err error) {
	config := *f.config
	config.Consumer.Offsets.Initial = initialOffset

	group, err := sarama.NewConsumerGroup(f.brokers, groupID, &config)
	if err != nil {
		return nil, err
	}

	adapter := newConsumerGroupAdapter(group, []string{topic})
	go adapter.consume(context.Background())

	return adapter, nil
}

func (f kafkaFactory) MakeSyncProducer() (producer sarama.SyncProducer, err error) {
	producer, err = sarama.NewSyncProducer(f.brokers, f.config)
	return
}

func (f kafkaFactory) MakeConsumerWorker(name string, consumer types.ClusterConsumer) types.ConsumerWorker {
	return NewConsumerWorker(name, consumer)
}

type consumerGroupAdapter struct {
	group         sarama.ConsumerGroup
	session       sarama.ConsumerGroupSession
	messages      chan *sarama.ConsumerMessage
	notifications chan *types.Notification
	errors        chan error
	topics        []string
}

func newConsumerGroupAdapter(group sarama.ConsumerGroup, topics []string) *consumerGroupAdapter {
	return &consumerGroupAdapter{
		group:         group,
		topics:        topics,
		messages:      make(chan *sarama.ConsumerMessage),
		notifications: make(chan *types.Notification, 1),
		errors:        make(chan error, 1),
	}
}

func (a *consumerGroupAdapter) consume(ctx context.Context) {
	for {
		if err := a.group.Consume(ctx, a.topics, a); err != nil {
			select {
			case a.errors <- err:
			default:
			}
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func (a *consumerGroupAdapter) Setup(session sarama.ConsumerGroupSession) error {
	a.session = session
	select {
	case a.notifications <- &types.Notification{Type: "rebalance_start"}:
	default:
	}
	return nil
}

func (a *consumerGroupAdapter) Cleanup(_ sarama.ConsumerGroupSession) error {
	select {
	case a.notifications <- &types.Notification{Type: "rebalance_end"}:
	default:
	}
	return nil
}

func (a *consumerGroupAdapter) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		select {
		case a.messages <- msg:
		case <-session.Context().Done():
			return nil
		}
	}
	return nil
}

func (a *consumerGroupAdapter) Messages() <-chan *sarama.ConsumerMessage {
	return a.messages
}

func (a *consumerGroupAdapter) Notifications() <-chan *types.Notification {
	return a.notifications
}

func (a *consumerGroupAdapter) Errors() <-chan error {
	return a.errors
}

func (a *consumerGroupAdapter) MarkOffset(msg *sarama.ConsumerMessage, _ string) {
	if a.session != nil {
		a.session.MarkMessage(msg, "")
	}
}

func (a *consumerGroupAdapter) CommitOffsets() error {
	if a.session != nil {
		a.session.Commit()
	}
	return nil
}

func (a *consumerGroupAdapter) Close() error {
	return a.group.Close()
}
