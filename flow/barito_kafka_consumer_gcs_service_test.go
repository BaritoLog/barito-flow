package flow

import (
	"regexp"
	"testing"
	"time"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/mock"
	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
)

type baritoKafkaConsumerGCSServiceTestObject struct {
	baritoKafkaConsumerGCSService
	kafkaAdmin           *mock.MockKafkaAdmin
	kafkaFactory         *mock.MockKafkaFactory
	consumerOuputFactory *mock.MockConsumerOutputFactory
}

func TestKafkaGCS_Start(t *testing.T) {
	t.Run("should call kafkaAdmin.RefreshTopics & Topics", func(t *testing.T) {
		obj, _, fn := getKafkaGCSConsumerObject(t)
		defer fn()

		obj.kafkaAdmin.EXPECT().RefreshTopics().Return(nil)
		obj.kafkaAdmin.EXPECT().Topics().Return([]string{})

		go obj.Start()
		time.Sleep(time.Second)
	})

	t.Run("should spawn worker depending on how much matching topics ", func(t *testing.T) {
		obj, ctrl, fn := getKafkaGCSConsumerObject(t)
		obj.topicPatternRegex = *regexp.MustCompile("topic.*")
		defer fn()

		// call the admin factory
		obj.kafkaAdmin.EXPECT().RefreshTopics().Return(nil)
		obj.kafkaAdmin.EXPECT().Topics().Return([]string{"topic1", "topic2", "foo"})

		// should call the consumerOutput factory
		gcs := mock.NewMockConsumerOutput(ctrl)
		gcs.EXPECT().Start().Return(nil).Times(2)
		gcs.EXPECT().AddOnFlushFunc(gomock.Any()).Times(2)
		obj.consumerOuputFactory.EXPECT().MakeConsumerOutputGCS("topic1").Return(gcs, nil)
		obj.consumerOuputFactory.EXPECT().MakeConsumerOutputGCS("topic2").Return(gcs, nil)

		// should call the clusterConsumer that returned from kafka factory
		clusterConsumer := mock.NewMockClusterConsumer(ctrl)
		clusterConsumer.EXPECT().Messages().AnyTimes()
		clusterConsumer.EXPECT().Errors().AnyTimes()
		clusterConsumer.EXPECT().Notifications().Return(nil).AnyTimes()

		// should call consumerWorker, when registering the message hook
		consumerWorker := mock.NewMockConsumerWorker(ctrl)
		consumerWorker.EXPECT().OnError(gomock.Any()).Times(2)
		consumerWorker.EXPECT().OnSuccess(gomock.Any()).Times(2)
		consumerWorker.EXPECT().Start().Times(2)

		// should call the kafka factory
		obj.kafkaFactory.EXPECT().MakeClusterConsumer("test_gcs_topic1", "topic1", sarama.OffsetOldest).Return(clusterConsumer, nil)
		obj.kafkaFactory.EXPECT().MakeClusterConsumer("test_gcs_topic2", "topic2", sarama.OffsetOldest).Return(clusterConsumer, nil)

		// should call the consumerWorker factory
		obj.kafkaFactory.EXPECT().MakeConsumerWorker("topic1", gomock.Any()).Return(consumerWorker)
		obj.kafkaFactory.EXPECT().MakeConsumerWorker("topic2", gomock.Any()).Return(consumerWorker)

		go obj.Start()
		time.Sleep(time.Second)

		require.Equal(t, 2, len(obj.workerMap), "should spawn 2 workers")
		require.Equal(t, 2, len(obj.gcsOutputMap), "should spawn 2 gcs outputs")

	})
}

func getKafkaGCSConsumerObject(t *testing.T) (baritoKafkaConsumerGCSServiceTestObject, *gomock.Controller, func()) {
	ctrl := gomock.NewController(t)
	kafkaAdmin := mock.NewMockKafkaAdmin(ctrl)
	kafkaFactory := mock.NewMockKafkaFactory(ctrl)
	consumerOuputFactory := mock.NewMockConsumerOutputFactory(ctrl)

	obj := baritoKafkaConsumerGCSServiceTestObject{
		kafkaAdmin:           kafkaAdmin,
		kafkaFactory:         kafkaFactory,
		consumerOuputFactory: consumerOuputFactory,
	}

	s := baritoKafkaConsumerGCSService{
		groupIDPrefix:        "test_gcs_",
		workerMap:            make(map[string]types.ConsumerWorker),
		gcsOutputMap:         make(map[string]types.ConsumerOutput),
		marshaler:            &jsonpb.Marshaler{},
		kafkaAdmin:           kafkaAdmin,
		kafkaFactory:         kafkaFactory,
		consumerOuputFactory: consumerOuputFactory,
		topicPatternRegex:    *regexp.MustCompile(".*"),
	}

	obj.baritoKafkaConsumerGCSService = s

	return obj, ctrl, func() {
		ctrl.Finish()
	}
}
