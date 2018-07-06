package flow

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/BaritoLog/barito-flow/mock_flow"
	"github.com/BaritoLog/go-boilerplate/saramatestkit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/golang/mock/gomock"
)

func TestHttpAgent_OnCreateTopicError(t *testing.T) {

	var topic string

	dummy := saramatestkit.NewSyncProducer()
	dummy.SendMessageFunc = func(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
		topic = msg.Topic
		return
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock_flow.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().CreateTopicIfNotExist(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(false, fmt.Errorf("some-error"))
	admin.EXPECT().Close().AnyTimes()

	agent := &baritoProducerService{
		producer:    dummy,
		topicSuffix: "_logs",
		bucket:      &dummyLeakyBucket{take: true},
		admin:       admin,
	}
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/",
		strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","kafka_partition": 3,"kafka_replication_factor": 1,"es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusServiceUnavailable)
}

func TestHttpAgent_ServeHTTP_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock_flow.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().CreateTopicIfNotExist(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(false, nil)
	admin.EXPECT().Close().AnyTimes()

	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndFail(fmt.Errorf("some error"))

	agent := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		bucket:      &dummyLeakyBucket{take: true},
		admin:       admin,
	}
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","kafka_partition": 3,"kafka_replication_factor": 1,"es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadGateway)
}

func TestHttpAgent_Start(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	producer.ExpectSendMessageAndSucceed()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock_flow.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().CreateTopicIfNotExist(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(false, nil)
	admin.EXPECT().Close().AnyTimes()

	agent := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		bucket:      &dummyLeakyBucket{take: true},
		admin:       admin,
	}

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`{"_ctx": {"kafka_topic": "some_topic","kafka_partition": 3,"kafka_replication_factor": 1,"es_index_prefix": "some-type","es_document_type": "some-type"}}`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, 200)
}

func TestHttpAgent_OnBadRequest(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock_flow.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Close().AnyTimes()

	agent := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		bucket:      &dummyLeakyBucket{take: true},
		admin:       admin,
	}
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`invalid-body`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadRequest)
}

func TestHttpAgent_OnLimitExceed(t *testing.T) {
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock_flow.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Close().AnyTimes()

	srv := &baritoProducerService{
		producer:    producer,
		topicSuffix: "_logs",
		bucket:      &dummyLeakyBucket{take: false},
	}
	defer srv.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(``))
	resp := RecordResponse(srv.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, 509)
}
