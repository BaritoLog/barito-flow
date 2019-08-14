package flow

import (
	"testing"

	pb "github.com/BaritoLog/barito-flow/proto"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/struct"
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
	document := ConvertTimberToEsDocumentString(timber, &jsonpb.Marshaler{})
	expected := "{\"@timestamp\":\"\"}"
	FatalIf(t, expected != document, "expected %s, received %s", expected, document)
}
