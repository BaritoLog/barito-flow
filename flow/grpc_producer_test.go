package flow

import (
	"fmt"
	"strings"
	"testing"

	"github.com/BaritoLog/barito-flow/mock"
	pb "github.com/BaritoLog/barito-flow/proto"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/golang/mock/gomock"
)

func TestBaritoProducerServer_Produce_OnLimitExceeded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	limiter := NewDummyRateLimiter()
	limiter.Expect_IsHitLimit_AlwaysTrue()

	srv := &baritoProducerServer{
		limiter: limiter,
	}

	_, err := srv.Produce(nil, pb.SampleTimberProto())
	FatalIfWrongGrpcError(t, onLimitExceededGrpc(), err)
}

func TestBaritoProducerServer_Produce_OnCreateTopicError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("create-topic-error"))

	producer := mock.NewMockSyncProducer(ctrl)
	limiter := NewDummyRateLimiter()

	srv := &baritoProducerServer{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	_, err := srv.Produce(nil, pb.SampleTimberProto())
	FatalIfWrongGrpcError(t, onCreateTopicErrorGrpc(fmt.Errorf("")), err)
}

func TestBaritoProducerServer_Produce_OnStoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(true)

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).
		Return(int32(0), int64(0), fmt.Errorf("some-error"))

	limiter := NewDummyRateLimiter()

	srv := &baritoProducerServer{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	_, err := srv.Produce(nil, pb.SampleTimberProto())
	FatalIfWrongGrpcError(t, onStoreErrorGrpc(fmt.Errorf("")), err)
}

func TestBaritoProducerServer_Produce_OnSuccess(t *testing.T) {
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

	srv := &baritoProducerServer{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	resp, err := srv.Produce(nil, pb.SampleTimberProto())
	FatalIfError(t, err)
	FatalIf(t, resp.GetTopic() != "some_topic_logs", "wrong result.Topic")
}

func FatalIfWrongGrpcError(t *testing.T, expected error, actual error) {
	expFields := strings.Fields(expected.Error())[:5]
	expStr := strings.Join(expFields, " ")

	actFields := strings.Fields(actual.Error())[:5]
	actStr := strings.Join(actFields, " ")

	if expStr != actStr {
		t.Errorf("expected gRPC response code %v, received %v.", expFields[4], actFields[4])
	}
}
