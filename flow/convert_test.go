package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	pb "github.com/bentol/barito-proto/producer"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

func TestConvertTimberToKafkaMessage(t *testing.T) {
	topic := "some-topic"

	timber := &pb.Timber{
		Timestamp: "2018-03-10T23:00:00Z",
	}

	message := ConvertTimberToKafkaMessage(timber, topic)
	FatalIf(t, message.Topic != topic, "%s != %s", message.Topic, topic)

	get, _ := message.Value.Encode()
	expected, _ := proto.Marshal(timber)
	FatalIf(t, string(get) != string(expected), "Wrong message value")
}

func TestConvertKafkaMessageToTimber_ProtoParseError(t *testing.T) {
	message := &sarama.ConsumerMessage{
		Topic: "some-topic",
		Value: []byte(`invalid_proto`),
	}

	_, err := ConvertKafkaMessageToTimber(message)
	FatalIfWrongError(t, err, string(ProtoParseError))
}

func TestConvertKafkaMessageToTimber(t *testing.T) {
	b, _ := proto.Marshal(&pb.Timber{})

	message := &sarama.ConsumerMessage{
		Topic: "some-topic",
		Value: b,
	}

	timber, err := ConvertKafkaMessageToTimber(message)
	FatalIfError(t, err)
	FatalIf(t, timber.GetContent() != nil, "Wrong timber[message]")
}

func TestConvertTimberToEsDocumentString(t *testing.T) {
	timber := pb.Timber{
		Content: &structpb.Struct{
			Fields: make(map[string]*structpb.Value),
		},
	}
	document, err := ConvertTimberToEsDocumentString(timber, &jsonpb.Marshaler{})
	FatalIfError(t, err)

	expected := "{\"@timestamp\":\"\"}"
	FatalIf(t, expected != document, "expected %s, received %s", expected, document)
}

func TestConvertTimberToEsDocumentString_ContentIsNil(t *testing.T) {
	timber := pb.Timber{
		Content: nil,
	}
	document, err := ConvertTimberToEsDocumentString(timber, &jsonpb.Marshaler{})
	FatalIf(t, TimberContentMissing != err, "expected %s, received %s", TimberContentMissing, document)

	expected := ""
	FatalIf(t, expected != document, "expected %s, received %s", expected, document)
}

func TestConvertTimberToEsDocumentString_FieldsIsNil(t *testing.T) {
	timber := pb.Timber{
		Content: &structpb.Struct{
			Fields: nil,
		},
	}
	document, err := ConvertTimberToEsDocumentString(timber, &jsonpb.Marshaler{})
	FatalIf(t, TimberFieldsMissing != err, "expected %s, received %s", TimberFieldsMissing, document)

	expected := ""
	FatalIf(t, expected != document, "expected %s, received %s", expected, document)
}

func TestConvertTimberToEsDocumentString_FieldsIsMissing(t *testing.T) {
	timber := pb.Timber{
		Content: &structpb.Struct{},
	}
	document, err := ConvertTimberToEsDocumentString(timber, &jsonpb.Marshaler{})
	FatalIf(t, TimberFieldsMissing != err, "expected %s, received %s", TimberFieldsMissing, document)

	expected := ""
	FatalIf(t, expected != document, "expected %s, received %s", expected, document)
}
func TestConvertTimberCollectionToKafkaMessage(t *testing.T) {
	topic := "test-topic"
	timberCollection := &pb.TimberCollection{
		Items: []*pb.Timber{
			{Timestamp: "2024-06-01T12:00:00Z"},
			{Timestamp: "2024-06-01T13:00:00Z"},
		},
	}

	message := ConvertTimberCollectionToKafkaMessage(timberCollection, topic)
	FatalIf(t, message.Topic != topic, "expected topic %s, got %s", topic, message.Topic)

	get, _ := message.Value.Encode()
	expected, _ := proto.Marshal(timberCollection)
	FatalIf(t, string(get) != string(expected), "Wrong message value")
}

func TestConvertTimberCollectionToKafkaMessage_EmptyCollection(t *testing.T) {
	topic := "empty-topic"
	timberCollection := &pb.TimberCollection{}

	message := ConvertTimberCollectionToKafkaMessage(timberCollection, topic)
	FatalIf(t, message.Topic != topic, "expected topic %s, got %s", topic, message.Topic)

	get, _ := message.Value.Encode()
	expected, _ := proto.Marshal(timberCollection)
	FatalIf(t, string(get) != string(expected), "Wrong message value for empty collection")
}
