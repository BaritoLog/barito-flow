package flow

import (
	"fmt"

	"github.com/BaritoLog/barito-flow/mock"
	"github.com/golang/mock/gomock"
)

type dummyKafkaFactory struct {
	MakeKafkaAdminFunc      func() (admin KafkaAdmin, err error)
	MakeClusterConsumerFunc func(groupID, topic string) (consumer ClusterConsumer, err error)
}

func NewDummyKafkaFactory() *dummyKafkaFactory {
	return &dummyKafkaFactory{
		MakeKafkaAdminFunc: func() (admin KafkaAdmin, err error) {
			return nil, nil
		},
		MakeClusterConsumerFunc: func(groupID, topic string) (worker ClusterConsumer, err error) {
			return nil, nil
		},
	}
}

func (f *dummyKafkaFactory) MakeKafkaAdmin() (admin KafkaAdmin, err error) {
	return f.MakeKafkaAdminFunc()
}
func (f *dummyKafkaFactory) MakeClusterConsumer(groupID, topic string) (worker ClusterConsumer, err error) {
	return f.MakeClusterConsumerFunc(groupID, topic)
}

func (f *dummyKafkaFactory) Expect_MakeClusterConsumer_AlwaysError(errMsg string) {
	f.MakeClusterConsumerFunc = func(groupID, topic string) (ClusterConsumer, error) {
		return nil, fmt.Errorf(errMsg)
	}
}

func (f *dummyKafkaFactory) Expect_MakeClusterConsumer_AlwaysSuccess(ctrl *gomock.Controller) {
	f.MakeClusterConsumerFunc = func(groupID, topic string) (ClusterConsumer, error) {
		consumer := mock.NewMockClusterConsumer(ctrl)
		consumer.EXPECT().Messages().AnyTimes()
		consumer.EXPECT().Notifications().AnyTimes()
		consumer.EXPECT().Errors().AnyTimes()
		consumer.EXPECT().Close()
		return consumer, nil
	}
}

func (f *dummyKafkaFactory) Expect_MakeClusterConsumer_ConsumerSpawnWorkerErrorCase(ctrl *gomock.Controller, newTopicEventName, errMsg string) {
	f.MakeClusterConsumerFunc = func(groupID, topic string) (ClusterConsumer, error) {
		if topic == newTopicEventName {
			consumer := mock.NewMockClusterConsumer(ctrl)
			consumer.EXPECT().Messages().AnyTimes()
			consumer.EXPECT().Notifications().AnyTimes()
			consumer.EXPECT().Errors().AnyTimes()
			consumer.EXPECT().Close()
			return consumer, nil
		}

		return nil, fmt.Errorf(errMsg)
	}
}

func (f *dummyKafkaFactory) Expect_MakeKafkaAdmin_AlwaysError(errMsg string) {
	f.MakeKafkaAdminFunc = func() (admin KafkaAdmin, err error) {
		return nil, fmt.Errorf(errMsg)
	}
}

func (f *dummyKafkaFactory) Expect_MakeKafkaAdmin_ConsumerSuccess(ctrl *gomock.Controller, topics []string) {
	f.MakeKafkaAdminFunc = func() (KafkaAdmin, error) {
		admin := mock.NewMockKafkaAdmin(ctrl)
		admin.EXPECT().Topics().Return(topics)
		admin.EXPECT().Close()
		return admin, nil
	}
}
