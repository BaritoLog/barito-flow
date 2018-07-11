package flow

import (
	"net/http"
	"net/http/httptest"
	"testing"

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
	factory.Expect_MakeKafkaAdmin_AlwaysError("some-error")

	service := NewBaritoConsumerService(factory, "groupID", "elasticURL", "topicSuffix", "newTopicEventName")
	err := service.Start()
	FatalIfWrongError(t, err, "Make kafka admin failed: some-error")
}

func TestBaritoConsumerService_MakeNewTopicWorkerError(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.Expect_MakeClusterConsumer_AlwaysError("some-error")

	service := NewBaritoConsumerService(factory, "groupID", "elasticURL", "topicSuffix", "newTopicEventName")
	err := service.Start()

	FatalIfWrongError(t, err, "Make new topic worker failed: some-error")
}

func TestBaritoConsumerService(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_ConsumerSuccess(ctrl, []string{"abc_logs"})
	factory.Expect_MakeClusterConsumer_AlwaysSuccess(ctrl)

	service := NewBaritoConsumerService(factory, "", "", "_logs", "")

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

func TestBaritoConsumerService_SpawnWorkerError(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_ConsumerSuccess(ctrl, []string{"abc_logs"})
	factory.Expect_MakeClusterConsumer_ConsumerSpawnWorkerErrorCase(ctrl, "new_topic_events", "some-error")

	service := &baritoConsumerService{
		factory:           factory,
		newTopicEventName: "new_topic_events",
	}

	err := service.Start()
	FatalIfError(t, err)

	defer service.Close()

	FatalIfWrongError(t, service.lastError, string(ErrSpawnWorker))
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

func TestBaritoConsumerService_onNewTopicEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeClusterConsumer_AlwaysSuccess(ctrl)

	service := &baritoConsumerService{
		factory:   factory,
		workerMap: make(map[string]ConsumerWorker),
	}
	defer service.Close()

	service.onNewTopicEvent(&sarama.ConsumerMessage{
		Value: []byte("some-new-topic"),
	})

	FatalIf(t, service.lastNewTopic != "some-new-topic", "wrong service.lastNewTopic")
}

func TestBaritoConsumerService_onNewTopicEvent_ErrorSpawnWorker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeClusterConsumer_AlwaysError("some-error")

	service := &baritoConsumerService{
		factory:   factory,
		workerMap: make(map[string]ConsumerWorker),
	}
	defer service.Close()

	service.onNewTopicEvent(&sarama.ConsumerMessage{
		Value: []byte("some-new-topic"),
	})

	FatalIfWrongError(t, service.lastError, string(ErrSpawnWorkerOnNewTopic))
}
