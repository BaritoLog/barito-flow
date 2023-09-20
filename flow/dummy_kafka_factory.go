package flow

import (
	"fmt"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/mock"
	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
)

type dummyKafkaFactory struct {
	MakeKafkaAdminFunc      func() (admin types.KafkaAdmin, err error)
	MakeClusterConsumerFunc func(groupID, topic string, initialOffset int64) (consumer types.ClusterConsumer, err error)
	MakeSyncProducerFunc    func() (producer sarama.SyncProducer, err error)
	MakeConsumerWorkerFunc  func(name string, consumer types.ClusterConsumer) types.ConsumerWorker
}

func NewDummyKafkaFactory() *dummyKafkaFactory {
	return &dummyKafkaFactory{
		MakeKafkaAdminFunc: func() (admin types.KafkaAdmin, err error) {
			return nil, nil
		},
		MakeClusterConsumerFunc: func(groupID, topic string, initialOffset int64) (worker types.ClusterConsumer, err error) {
			return nil, nil
		},
		MakeSyncProducerFunc: func() (producer sarama.SyncProducer, err error) {
			return nil, nil
		},
		MakeConsumerWorkerFunc: func(name string, consumer types.ClusterConsumer) types.ConsumerWorker {
			return nil
		},
	}
}

func (f *dummyKafkaFactory) MakeKafkaAdmin() (admin types.KafkaAdmin, err error) {
	return f.MakeKafkaAdminFunc()
}
func (f *dummyKafkaFactory) MakeClusterConsumer(groupID, topic string, initialOffset int64) (worker types.ClusterConsumer, err error) {
	return f.MakeClusterConsumerFunc(groupID, topic, initialOffset)
}

func (f *dummyKafkaFactory) MakeSyncProducer() (producer sarama.SyncProducer, err error) {
	return f.MakeSyncProducerFunc()
}

func (f *dummyKafkaFactory) MakeConsumerWorker(name string, consumer types.ClusterConsumer) types.ConsumerWorker {
	return f.MakeConsumerWorkerFunc(name, consumer)
}

func (f *dummyKafkaFactory) Expect_MakeClusterConsumer_AlwaysError(errMsg string) {
	f.MakeClusterConsumerFunc = func(groupID, topic string, initialOffset int64) (types.ClusterConsumer, error) {
		return nil, fmt.Errorf(errMsg)
	}
}

func (f *dummyKafkaFactory) Expect_MakeClusterConsumer_AlwaysSuccess(ctrl *gomock.Controller) {
	f.MakeClusterConsumerFunc = func(groupID, topic string, initialOffset int64) (types.ClusterConsumer, error) {
		consumer := mock.NewMockClusterConsumer(ctrl)
		consumer.EXPECT().Messages().AnyTimes()
		consumer.EXPECT().Notifications().AnyTimes()
		consumer.EXPECT().Errors().AnyTimes()
		consumer.EXPECT().Close().AnyTimes()
		return consumer, nil
	}
}

func (f *dummyKafkaFactory) Expect_MakeClusterConsumer_ConsumerSpawnWorkerErrorCase(ctrl *gomock.Controller, newTopicEventName, errMsg string) {
	f.MakeClusterConsumerFunc = func(groupID, topic string, initialOffset int64) (types.ClusterConsumer, error) {
		if topic == newTopicEventName {
			consumer := mock.NewMockClusterConsumer(ctrl)
			consumer.EXPECT().Messages().AnyTimes()
			consumer.EXPECT().Notifications().AnyTimes()
			consumer.EXPECT().Errors().AnyTimes()
			consumer.EXPECT().Close().AnyTimes()
			return consumer, nil
		}

		return nil, fmt.Errorf(errMsg)
	}
}

func (f *dummyKafkaFactory) Expect_MakeKafkaAdmin_AlwaysError(errMsg string) {
	f.MakeKafkaAdminFunc = func() (admin types.KafkaAdmin, err error) {
		return nil, fmt.Errorf(errMsg)
	}
}

func (f *dummyKafkaFactory) Expect_MakeKafkaAdmin_ConsumerServiceSuccess(ctrl *gomock.Controller, topics []string) {
	f.MakeKafkaAdminFunc = func() (types.KafkaAdmin, error) {
		admin := mock.NewMockKafkaAdmin(ctrl)
		admin.EXPECT().Topics().Return(topics).AnyTimes()
		admin.EXPECT().Close().AnyTimes()
		return admin, nil
	}
}

func (f *dummyKafkaFactory) Expect_MakeKafkaAdmin_ProducerServiceSuccess(ctrl *gomock.Controller, topics []string) {
	f.MakeKafkaAdminFunc = func() (types.KafkaAdmin, error) {
		admin := mock.NewMockKafkaAdmin(ctrl)
		admin.EXPECT().Close()
		return admin, nil
	}
}

func (f *dummyKafkaFactory) Expect_MakeSyncProducerFunc_AlwaysError(errMsg string) {
	f.MakeSyncProducerFunc = func() (producer sarama.SyncProducer, err error) {
		return nil, fmt.Errorf(errMsg)
	}
}
