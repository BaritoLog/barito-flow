package flow

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BaritoLog/barito-flow/mock"
	pb "github.com/BaritoLog/barito-flow/proto"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

func TestBaritConsumerService_MakeKafkaAdminError(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_AlwaysError("some-error")
	esConfig := NewEsConfig("BulkProcessor", 100, time.Duration(1000), false)

	service := NewBaritoConsumerService(factory, "groupID", "elasticURL", "topicSuffix", 1, 10, "newTopicEventName", "", esConfig)
	err := service.Start()
	FatalIfWrongError(t, err, "Make kafka admin failed: Error connecting to kafka, retry limit reached")
}

func TestBaritoConsumerService_MakeNewTopicWorkerError(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.Expect_MakeClusterConsumer_AlwaysError("some-error")
	esConfig := NewEsConfig("BulkProcessor", 100, time.Duration(1000), false)

	service := NewBaritoConsumerService(factory, "groupID", "elasticURL", "topicSuffix", 0, 10, "newTopicEventName", "", esConfig)
	err := service.Start()

	FatalIfWrongError(t, err, "Make new topic worker failed: some-error")
}

func TestBaritoConsumerService(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_ConsumerServiceSuccess(ctrl, []string{"abc_logs"})
	factory.Expect_MakeClusterConsumer_AlwaysSuccess(ctrl)
	esConfig := NewEsConfig("BulkProcessor", 100, time.Duration(1000), false)

	var v interface{} = NewBaritoConsumerService(factory, "", "", "_logs", 0, 10, "", "", esConfig)
	service := v.(*baritoConsumerService)

	err := service.Start()
	FatalIfError(t, err)
	FatalIf(t, !strings.HasPrefix(service.eventWorkerGroupID, PrefixEventGroupID), "eventWorkerGroup should be have prefix")

	// service.Start() execute goroutine, so wait 1ms to make sure it come in to mainLoop
	timekit.Sleep("1ms")

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
	factory.Expect_MakeKafkaAdmin_ConsumerServiceSuccess(ctrl, []string{"abc_logs"})
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

func TestBaritoConsumerService_onStoreTimber_ErrorConvertKafkaMessage(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`{}`))
	defer ts.Close()

	service := &baritoConsumerService{
		elasticUrl: ts.URL,
	}

	invalidKafkaMessage := &sarama.ConsumerMessage{
		Value: []byte(`invalid_proto`),
	}

	service.onStoreTimber(invalidKafkaMessage)
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

	retrier := service.elasticRetrier()
	esConfig := NewEsConfig("SingleInsert", 1, time.Duration(1000), false)
	elastic, _ := NewElastic(retrier, esConfig, ts.URL)
	service.esClient = &elastic

	timberBytes, _ := proto.Marshal(pb.SampleTimberProto())

	service.onStoreTimber(&sarama.ConsumerMessage{
		Value: timberBytes,
	})
	FatalIfWrongError(t, service.lastError, string(ErrStore))
}

func TestBaritoConsumerService_onStoreTimber(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`{}`))
	defer ts.Close()

	service := &baritoConsumerService{
		elasticUrl: ts.URL,
	}

	retrier := service.elasticRetrier()
	esConfig := NewEsConfig("SingleInsert", 1, time.Duration(1000), false)
	elastic, _ := NewElastic(retrier, esConfig, ts.URL)
	service.esClient = &elastic

	timberBytes, _ := proto.Marshal(pb.SampleTimberProto())

	service.onStoreTimber(&sarama.ConsumerMessage{
		Value: timberBytes,
	})
	FatalIfError(t, service.lastError)

	nilTimber := pb.Timber{}
	lastTimberContextIsNil := (service.lastTimber.GetContext() == nilTimber.GetContext())
	lastTimberContentIsNil := (service.lastTimber.GetContent() == nilTimber.GetContent())
	FatalIf(t, lastTimberContextIsNil && lastTimberContentIsNil, "lastTimber can't be nil")
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

func TestBaritoConsumerService_onNewTopicEvent_IgnoreIfTopicExist(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeClusterConsumer_AlwaysSuccess(ctrl)

	worker := mock.NewMockConsumerWorker(ctrl)
	worker.EXPECT().Stop().AnyTimes()

	workerMap := map[string]ConsumerWorker{
		"topic001": worker,
	}

	service := &baritoConsumerService{
		factory:   factory,
		workerMap: workerMap,
	}
	defer service.Close()

	service.onNewTopicEvent(&sarama.ConsumerMessage{Value: []byte("topic002")})
	service.onNewTopicEvent(&sarama.ConsumerMessage{Value: []byte("topic001")})

	FatalIf(t, service.lastNewTopic == "topic001", "lastNewTopic should be not topic001")
}

func TestHaltAllWorker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_ConsumerServiceSuccess(ctrl, []string{"abc_logs"})
	factory.Expect_MakeClusterConsumer_AlwaysSuccess(ctrl)
	esConfig := NewEsConfig("BulkProcessor", 100, time.Duration(1000), false)

	var v interface{} = NewBaritoConsumerService(factory, "", "", "_logs", 0, 10, "", "", esConfig)
	service := v.(*baritoConsumerService)

	err := service.Start()
	FatalIfError(t, err)
	FatalIf(t, !strings.HasPrefix(service.eventWorkerGroupID, PrefixEventGroupID), "eventWorkerGroup should be have prefix")

	// service.Start() execute goroutine, so wait 1ms to make sure it come in to mainLoop
	timekit.Sleep("1ms")

	worker := service.NewTopicEventWorker()
	workerMap := service.WorkerMap()

	service.HaltAllWorker()

	FatalIf(t, !service.isHalt, "Consumer Worker should be halted")
	FatalIf(t, !worker.IsStart(), "New Topic Event Worker should be halted")

	for _, w := range workerMap {
		FatalIf(t, !w.IsStart(), "Worker should be halted")
	}
}

func TestResumeWorker(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_ConsumerServiceSuccess(ctrl, []string{"abc_logs"})
	factory.Expect_MakeClusterConsumer_AlwaysSuccess(ctrl)
	esConfig := NewEsConfig("BulkProcessor", 100, time.Duration(1000), false)

	var v interface{} = NewBaritoConsumerService(factory, "", "", "_logs", 0, 10, "", "", esConfig)
	service := v.(*baritoConsumerService)

	err := service.ResumeWorker()
	FatalIfError(t, err)
	FatalIf(t, service.isHalt, "Consumer Worker should be started")
	// service.Start() execute goroutine, so wait 1ms to make sure it come in to mainLoop
	timekit.Sleep("1ms")
	defer service.Close()
}
