package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/BaritoLog/barito-flow/mock"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/golang/mock/gomock"
)

func TestBaritoProducerService_ServeHTTP_OnLimitExceed(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	limiter := NewDummyRateLimiter()
	limiter.Expect_IsHitLimit_AlwaysTrue()

	srv := &baritoProducerService{
		limiter: limiter,
	}

	req, _ := http.NewRequest("POST", "/", bytes.NewReader(sampleRawTimber()))
	resp := RecordResponse(srv.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, 509)
}

func TestBaritoProducerService_ServeHTTP_Batch_OnLimitExceed(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	limiter := NewDummyRateLimiter()
	limiter.Expect_IsHitLimit_AlwaysTrue()

	srv := &baritoProducerService{
		limiter: limiter,
	}

	req, _ := http.NewRequest("POST", "/produce_batch", bytes.NewReader(sampleRawTimberCollection()))
	resp := RecordResponse(srv.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, 509)
}

func TestBaritoProducerService_ServeHTTP_OnBadRequest(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().Close()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Close().AnyTimes()

	agent := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
	}
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`invalid-body`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadRequest)
}

func TestBaritoProducerService_ServeHTTP_OnStoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(true)

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).
		Return(int32(0), int64(0), fmt.Errorf("some-error"))

	limiter := NewDummyRateLimiter()

	agent := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	req, _ := http.NewRequest("POST", "/", bytes.NewReader(sampleRawTimber()))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadGateway)
}

func TestBaritoProducerService_ServeHTTP_OnCreateTopicError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("create-topic-error"))

	producer := mock.NewMockSyncProducer(ctrl)

	limiter := NewDummyRateLimiter()

	agent := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	req, _ := http.NewRequest("POST", "/", bytes.NewReader(sampleRawTimber()))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusServiceUnavailable)
}

func TestBaritoProducerService_ServeHTTP_OnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	admin.EXPECT().AddTopic(gomock.Any())

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).AnyTimes()

	limiter := NewDummyRateLimiter()

	agent := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	req, _ := http.NewRequest("POST", "/", bytes.NewReader(sampleRawTimber()))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusOK)

	var result ProduceResult
	b, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(b, &result)

	FatalIf(t, result.Topic != "some_topic_logs", "wrong result.Topic")
}

func TestBaritoProducerService_ServeHTTP_ProduceBatch_OnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	admin.EXPECT().AddTopic(gomock.Any())
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	admin.EXPECT().AddTopic(gomock.Any())

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).AnyTimes()

	limiter := NewDummyRateLimiter()

	agent := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	req, _ := http.NewRequest("POST", "/produce_batch", bytes.NewReader(sampleRawTimberCollection()))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusOK)

	var result ProduceResult
	b, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(b, &result)

	FatalIf(t, result.Topic != "some_topic_logs", "wrong result.Topic")
}

func TestBaritoProducerService_Start_ErrorMakeSyncProducer(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.Expect_MakeSyncProducerFunc_AlwaysError("some-error")

	service := NewBaritoProducerService(factory, "addr", 1, 1, "_logs", 1, 10, "new_topic_events")
	err := service.Start()

	FatalIfWrongError(t, err, "Make sync producer failed: Error connecting to kafka, retry limit reached")
}

func TestBaritoProducerService_Start_ErrorMakeKafkaAdmin(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_AlwaysError("some-error")

	service := NewBaritoProducerService(factory, "addr", 1, 1, "_logs", 1, 10, "new_topic_events")
	err := service.Start()

	FatalIfWrongError(t, err, "Make kafka admin failed: Error connecting to kafka, retry limit reached")
}

func TestBaritoProducerService_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_ProducerServiceSuccess(ctrl, []string{})

	service := &baritoProducerService{
		factory:       factory,
		addr:          ":24400",
		topicSuffix:   "_logs",
		newEventTopic: "new_topic_event",
	}

	var err error
	go func() {
		err = service.Start()
	}()
	defer service.Close()

	FatalIfError(t, err)

	timekit.Sleep("1ms")
	FatalIf(t, !service.limiter.IsStart(), "rate limiter must be start")

	resp, err := http.Get("http://:24400")
	FatalIfError(t, err)
	FatalIfWrongResponseStatus(t, resp, http.StatusBadRequest)
}
