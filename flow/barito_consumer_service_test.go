package flow

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/BaritoLog/barito-flow/mock"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

func TestBaritConsumerService_MakeKafkaAdminError(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.MakeKafkaAdminFunc = func() (admin KafkaAdmin, err error) {
		return nil, fmt.Errorf("some-error")
	}

	service := NewBaritoConsumerService(factory, "groupID", "elasticURL", "topicSuffix", "newTopicEventName")
	err := service.Start()
	FatalIfWrongError(t, err, "Make kafka admin failed: some-error")
}

func TestBaritoConsumerService_MakeConsumerWorkerError(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.MakeKafkaAdminFunc = func() (KafkaAdmin, error) {
		return nil, nil
	}
	factory.MakeClusterConsumerFunc = func(groupID, topic string) (ClusterConsumer, error) {
		return nil, fmt.Errorf("some-error")
	}

	service := NewBaritoConsumerService(factory, "groupID", "elasticURL", "topicSuffix", "newTopicEventName")
	err := service.Start()

	FatalIfWrongError(t, err, "Make new topic worker failed: some-error")
}

func TestBaritoConsumerService(t *testing.T) {
	newTopicEventName := "new_topic_events"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.MakeKafkaAdminFunc = func() (KafkaAdmin, error) {
		admin := mock.NewMockKafkaAdmin(ctrl)
		admin.EXPECT().Topics().Return([]string{"abc_logs"})
		admin.EXPECT().Close()
		return admin, nil
	}
	factory.MakeClusterConsumerFunc = func(groupID, topic string) (ClusterConsumer, error) {
		consumer := mock.NewMockClusterConsumer(ctrl)
		consumer.EXPECT().Messages().AnyTimes()
		consumer.EXPECT().Notifications().AnyTimes()
		consumer.EXPECT().Errors().AnyTimes()
		consumer.EXPECT().Close()
		return consumer, nil
	}

	service := NewBaritoConsumerService(factory, "groupID", "elasticURL", "_logs", newTopicEventName)

	err := service.Start()
	FatalIfError(t, err)

	defer service.Close()

	worker := service.NewTopicEventWorker()
	FatalIf(t, worker == nil, "newTopicEventWorker can't be nil")
	FatalIf(t, !worker.IsStart(), "newTopicEventWorker is not starting")

	workerMap := service.WorkerMap()
	FatalIf(t, len(workerMap) != 1, "wrong worker map")

	worker, ok := workerMap["abc_logs"]
	FatalIf(t, !ok, "worker of topic abc_logs is missing")
	FatalIf(t, !worker.IsStart(), "worker of topic abc_logs is not starting")
}

func TestBaritoConsumerService_onStoreTimber_ErrorElasticsearchClient(t *testing.T) {
	service := &baritoConsumerService{}

	service.onStoreTimber(&sarama.ConsumerMessage{})
	FatalIfWrongError(t, service.lastError, string(ErrElasticsearchClient))
}

func TestBaritoConsumerService_onStoreTimber_ErrorConvertKafkaMessage(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`{}`))
	defer ts.Close()

	service := &baritoConsumerService{
		elasticUrl: ts.URL,
	}

	service.onStoreTimber(&sarama.ConsumerMessage{})
	FatalIfWrongError(t, service.lastError, string(ErrConvertKafkaMessage))
}

func TestBaritoConsumerService_onStoreTimber_ErrorStore(t *testing.T) {
	ts := httptest.NewServer(&ELasticTestHandler{
		ExistAPIStatus:  http.StatusOK,
		CreateAPIStatus: http.StatusOK,
		PostAPIStatus:   http.StatusBadRequest,
	})
	defer ts.Close()

	service := &baritoConsumerService{
		elasticUrl: ts.URL,
	}

	service.onStoreTimber(&sarama.ConsumerMessage{
		Value: sampleRawTimber(),
	})
	FatalIfWrongError(t, service.lastError, string(ErrStore))
}

func TestBaritoConsumerService_onStoreTimber(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`{}`))
	defer ts.Close()

	service := &baritoConsumerService{
		elasticUrl: ts.URL,
	}

	service.onStoreTimber(&sarama.ConsumerMessage{
		Value: sampleRawTimber(),
	})
	FatalIfError(t, service.lastError)
	FatalIf(t, service.lastTimber == nil, "lastTimber can't be nil")
}
