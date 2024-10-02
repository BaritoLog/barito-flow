package flow

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/BaritoLog/barito-flow/prome"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/BaritoLog/barito-flow/mock"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	pb "github.com/bentol/barito-proto/producer"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
)

func resetPrometheusMetrics() {
	registry := prometheus.NewRegistry()
	prometheus.DefaultGatherer = registry
	prometheus.DefaultRegisterer = registry

	prome.InitProducerInstrumentation()
	prome.InitConsumerInstrumentation()
}

func TestProducerService_Produce_OnLimitExceeded(t *testing.T) {
	resetPrometheusMetrics()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	limiter := NewDummyRateLimiter()
	limiter.Expect_IsHitLimit_AlwaysTrue()

	srv := &producerService{
		limiter: limiter,
	}

	_, err := srv.Produce(nil, pb.SampleTimberProto())
	FatalIfWrongGrpcError(t, onLimitExceededGrpc(), err)

	expected := `
		# HELP barito_producer_tps_exceeded_total Number of TPS exceeded event
		# TYPE barito_producer_tps_exceeded_total counter
		barito_producer_tps_exceeded_total{topic="some_topic"} 1
	`
	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_tps_exceeded_total"))
}

func TestProducerService_ProduceBatch_OnLimitExceeded(t *testing.T) {
	resetPrometheusMetrics()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	limiter := NewDummyRateLimiter()
	limiter.Expect_IsHitLimit_AlwaysTrue()

	srv := &producerService{
		limiter: limiter,
	}

	_, err := srv.ProduceBatch(nil, pb.SampleTimberCollectionProto())
	FatalIfWrongGrpcError(t, onLimitExceededGrpc(), err)

	expected := `
	# HELP barito_producer_tps_exceeded_total Number of TPS exceeded event
	# TYPE barito_producer_tps_exceeded_total counter
	barito_producer_tps_exceeded_total{topic="some_topic"} 2
`
	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_tps_exceeded_total"))
}

func TestProducerService_Produce_OnCreateTopicError(t *testing.T) {
	resetPrometheusMetrics()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("create-topic-error"))

	producer := mock.NewMockSyncProducer(ctrl)
	limiter := NewDummyRateLimiter()

	srv := &producerService{
		producer:    producer,
		topicPrefix: "prefix_",
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	_, err := srv.Produce(nil, pb.SampleTimberProto())
	FatalIfWrongGrpcError(t, onCreateTopicErrorGrpc(fmt.Errorf("")), err)

	expected := `
		# HELP barito_producer_kafka_message_stored_total Number of message stored to kafka
		# TYPE barito_producer_kafka_message_stored_total counter
		barito_producer_kafka_message_stored_total{error_type="create_topic",topic="prefix_some_topic_logs"} 1
	`
	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_kafka_message_stored_total"))
}

func TestProducerService_Produce_OnStoreError(t *testing.T) {
	resetPrometheusMetrics()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(true)

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).
		Return(int32(0), int64(0), fmt.Errorf("some-error"))

	limiter := NewDummyRateLimiter()

	srv := &producerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	_, err := srv.Produce(nil, pb.SampleTimberProto())
	FatalIfWrongGrpcError(t, onStoreErrorGrpc(fmt.Errorf("")), err)

	expected := `
		# HELP barito_producer_kafka_message_stored_total Number of message stored to kafka
		# TYPE barito_producer_kafka_message_stored_total counter
		barito_producer_kafka_message_stored_total{error_type="send_log",topic="some_topic_logs"} 1
	`
	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_kafka_message_stored_total"))
}

func TestProducerService_Produce_OnSuccess(t *testing.T) {
	resetPrometheusMetrics()

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

	srv := &producerService{
		producer:           producer,
		topicPrefix:        "prefix_",
		topicSuffix:        "_logs",
		admin:              admin,
		limiter:            limiter,
		kafkaMaxRetry:      5,
		kafkaRetryInterval: 10,
	}

	resp, err := srv.Produce(nil, pb.SampleTimberProto())
	FatalIfError(t, err)
	FatalIf(t, resp.GetTopic() != "some_topic_logs", "wrong result.Topic")

	expected := `
		# HELP barito_producer_kafka_message_stored_total Number of message stored to kafka
		# TYPE barito_producer_kafka_message_stored_total counter
		barito_producer_kafka_message_stored_total{error_type="",topic="prefix_some_topic_logs"} 1
	`
	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_kafka_message_stored_total"))
}

func TestProducerService_Produce_IgnoreKafkaOptions(t *testing.T) {
	resetPrometheusMetrics()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), int32(-1), int16(-1)).
		Return(nil)
	admin.EXPECT().AddTopic(gomock.Any())

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).AnyTimes()

	limiter := NewDummyRateLimiter()

	srv := &producerService{
		producer:           producer,
		topicPrefix:        "prefix_",
		topicSuffix:        "_logs",
		admin:              admin,
		limiter:            limiter,
		kafkaMaxRetry:      5,
		kafkaRetryInterval: 10,
		ignoreKafkaOptions: true,
	}
	srv.Produce(nil, pb.SampleTimberProto())
}

func TestProducerService_ProduceBatch_OnSuccess(t *testing.T) {
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

	srv := &producerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	resp, err := srv.ProduceBatch(nil, pb.SampleTimberCollectionProto())
	FatalIfError(t, err)
	FatalIf(t, resp.GetTopic() != "some_topic_logs", "wrong result.Topic")
}

func TestProducerService_Start_ErrorMakeSyncProducer(t *testing.T) {
	resetPrometheusMetrics()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeSyncProducerFunc_AlwaysError("some-error")

	limiter := NewDummyRateLimiter()

	producerParams := map[string]interface{}{
		"factory":                factory,
		"grpcAddr":               "grpc",
		"restAddr":               "rest",
		"rateLimitResetInterval": 1,
		"topicSuffix":            "_logs",
		"kafkaMaxRetry":          2,
		"kafkaRetryInterval":     1,
		"newEventTopic":          "new_topic_events",
		"grpcMaxRecvMsgSize":     20000000,
		"ignoreKafkaOptions":     false,
		"limiter":                limiter,
	}

	service := NewProducerService(producerParams)
	err := service.Start()

	FatalIfWrongError(t, err, "Make sync producer failed: Error connecting to kafka, retry limit reached")
	expected := `
		# HELP barito_producer_kafka_client_failed Number of client failed to connect to kafka
		# TYPE barito_producer_kafka_client_failed counter
		barito_producer_kafka_client_failed 2
	`
	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_kafka_client_failed"))
}

func TestProducerService_Start_ErrorMakeKafkaAdmin(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_AlwaysError("some-error")

	limiter := NewDummyRateLimiter()

	producerParams := map[string]interface{}{
		"factory":                factory,
		"grpcAddr":               "grpc",
		"restAddr":               "rest",
		"rateLimitResetInterval": 1,
		"topicSuffix":            "_logs",
		"kafkaMaxRetry":          1,
		"kafkaRetryInterval":     10,
		"newEventTopic":          "new_topic_events",
		"grpcMaxRecvMsgSize":     20000000,
		"ignoreKafkaOptions":     false,
		"limiter":                limiter,
	}

	service := NewProducerService(producerParams)
	err := service.Start()

	FatalIfWrongError(t, err, "Make kafka admin failed: Error connecting to kafka, retry limit reached")
}

func TestProducerService_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_ProducerServiceSuccess(ctrl, []string{})

	limiter := NewDummyRateLimiter()

	service := &producerService{
		factory:       factory,
		grpcAddr:      ":24400",
		topicSuffix:   "_logs",
		newEventTopic: "new_topic_event",
		limiter:       limiter,
	}

	var err error
	go func() {
		err = service.Start()
	}()
	defer service.Close()

	FatalIfError(t, err)

	timekit.Sleep("1ms")
	FatalIf(t, !service.limiter.IsStart(), "rate limiter must be start")
}

func TestProducerService_Produce_OnByteIngested(t *testing.T) {
	resetPrometheusMetrics()

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

	srv := &producerService{
		producer:           producer,
		topicSuffix:        "_logs",
		admin:              admin,
		limiter:            limiter,
		kafkaMaxRetry:      5,
		kafkaRetryInterval: 10,
	}

	sampleTimber := pb.SampleTimberProto()
	sampleTimber.Timestamp = time.Now().UTC().Format(time.RFC3339)

	resp, err := srv.Produce(nil, sampleTimber)
	FatalIfError(t, err)
	FatalIf(t, resp.GetTopic() != "some_topic_logs", "wrong result.Topic")

	expectedByte, _ := proto.Marshal(sampleTimber)
	expectedByteSize := float64(len(expectedByte))

	expected := fmt.Sprintf(`
		# HELP barito_producer_produced_total_log_bytes Total log bytes being ingested by the producer
		# TYPE barito_producer_produced_total_log_bytes counter
		barito_producer_produced_total_log_bytes{app_name="some_topic"} %f
	`, expectedByteSize)

	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_produced_total_log_bytes"))
}

func TestProducerService_ProduceBatch_OnByteIngested(t *testing.T) {
	resetPrometheusMetrics()

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

	srv := &producerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	sampleTimberCollection := pb.SampleTimberCollectionProto()
	var expectedByteSize float64

	for _, sampleTimber := range sampleTimberCollection.GetItems() {
		sampleTimber.Context = sampleTimberCollection.GetContext()
		sampleTimber.Timestamp = time.Now().UTC().Format(time.RFC3339)
		expectedByte, _ := proto.Marshal(sampleTimber)
		expectedByteSize += float64(len(expectedByte))
	}

	resp, err := srv.ProduceBatch(nil, sampleTimberCollection)
	FatalIfError(t, err)
	FatalIf(t, resp.GetTopic() != "some_topic_logs", "wrong result.Topic")

	expected := fmt.Sprintf(`
		# HELP barito_producer_produced_total_log_bytes Total log bytes being ingested by the producer
		# TYPE barito_producer_produced_total_log_bytes counter
		barito_producer_produced_total_log_bytes{app_name="some_topic"} %f
	`, expectedByteSize)

	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_produced_total_log_bytes"))
}

func TestProducerService_Produce_TPSExceededBytes(t *testing.T) {
	resetPrometheusMetrics()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	producer := mock.NewMockSyncProducer(ctrl)

	limiter := NewDummyRateLimiter()
	limiter.Expect_IsHitLimit_AlwaysTrue()

	srv := &producerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	sampleTimber := pb.SampleTimberProto()
	sampleTimber.Timestamp = time.Now().UTC().Format(time.RFC3339)

	_, err := srv.Produce(nil, sampleTimber)
	FatalIfWrongGrpcError(t, onLimitExceededGrpc(), err)

	expectedByte, _ := proto.Marshal(sampleTimber)
	expectedByteSize := float64(len(expectedByte))

	expected := fmt.Sprintf(`
		# HELP barito_producer_tps_exceeded_log_bytes Log bytes of TPS exceeded requests
		# TYPE barito_producer_tps_exceeded_log_bytes counter
		barito_producer_tps_exceeded_log_bytes{app_name="some_topic"} %f
	`, expectedByteSize)

	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_tps_exceeded_log_bytes"))
}

func TestProducerService_ProduceBatch_TPSExceededBytes(t *testing.T) {
	resetPrometheusMetrics()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	producer := mock.NewMockSyncProducer(ctrl)

	limiter := NewDummyRateLimiter()
	limiter.Expect_IsHitLimit_AlwaysTrue()

	srv := &producerService{
		producer:    producer,
		topicSuffix: "_logs",
		admin:       admin,
		limiter:     limiter,
	}

	sampleTimberCollection := pb.SampleTimberCollectionProto()
	var expectedByteSize float64

	for _, sampleTimber := range sampleTimberCollection.GetItems() {
		sampleTimber.Context = sampleTimberCollection.GetContext()
		sampleTimber.Timestamp = time.Now().UTC().Format(time.RFC3339)
		expectedByte, _ := proto.Marshal(sampleTimber)
		expectedByteSize += float64(len(expectedByte))
	}

	_, err := srv.ProduceBatch(nil, sampleTimberCollection)
	FatalIfWrongGrpcError(t, onLimitExceededGrpc(), err)

	expected := fmt.Sprintf(`
		# HELP barito_producer_tps_exceeded_log_bytes Log bytes of TPS exceeded requests
		# TYPE barito_producer_tps_exceeded_log_bytes counter
		barito_producer_tps_exceeded_log_bytes{app_name="some_topic"} %f
	`, expectedByteSize)

	FatalIfError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "barito_producer_tps_exceeded_log_bytes"))
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
