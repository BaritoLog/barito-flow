package flow

import (
	"fmt"
	"testing"

	"github.com/BaritoLog/barito-flow/mock"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

func TestBaritConsumerService_MakeKafkaAdminError(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.MakeKafkaAdminFunc = func() (admin KafkaAdmin, err error) {
		return nil, fmt.Errorf("some-admin-error")
	}

	_, err := NewBaritoConsumerService(factory, "groupID", "elasticURL", "topicSuffix", "newTopicEventName")

	FatalIfWrongError(t, err, "some-admin-error")
}

func TestBaritoConsumerService_MakeConsumerWorkerError(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.MakeKafkaAdminFunc = func() (KafkaAdmin, error) {
		return nil, nil
	}
	factory.MakeConsumerWorkerFunc = func(groupID, topic string) (ConsumerWorker, error) {
		return nil, fmt.Errorf("some-consumer-error")
	}

	_, err := NewBaritoConsumerService(factory, "groupID", "elasticURL", "topicSuffix", "newTopicEventName")

	FatalIfWrongError(t, err, "some-consumer-error")
}

func TestBaritoConsumerService(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.MakeKafkaAdminFunc = func() (KafkaAdmin, error) {
		admin := mock.NewMockKafkaAdmin(ctrl)
		admin.EXPECT().Topics().Return([]string{"topic_logs"})
		admin.EXPECT().Close()
		return admin, nil
	}
	factory.MakeConsumerWorkerFunc = func(groupID, topic string) (ConsumerWorker, error) {
		worker := mock.NewMockConsumerWorker(ctrl)
		worker.EXPECT().OnSuccess(gomock.Any()).AnyTimes()
		worker.EXPECT().OnError(gomock.Any()).AnyTimes()
		worker.EXPECT().OnNotification(gomock.Any()).AnyTimes()
		worker.EXPECT().Start().AnyTimes()
		worker.EXPECT().Close().AnyTimes()
		return worker, nil
	}

	service, err := NewBaritoConsumerService(factory, "groupID", "elasticURL", "_logs", "newTopicEvent")
	FatalIfError(t, err)

	service.Start()
	defer service.Close()

	// TODO:

}
