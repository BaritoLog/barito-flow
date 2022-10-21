package flow

import (
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	stpb "github.com/golang/protobuf/ptypes/struct"
	pb "github.com/vwidjaya/barito-proto/producer"
)

const (
	JsonParseError       = errkit.Error("JSON Parse Error")
	ProtoParseError      = errkit.Error("Protobuf Parse Error")
	TimberContentMissing = errkit.Error("Timber Content Missing Error")
	TimberFieldsMissing  = errkit.Error("Timber Field Missing Error")
)

func ConvertTimberToKafkaMessage(timber *pb.Timber, topic string) *sarama.ProducerMessage {
	b, _ := proto.Marshal(timber)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
}

func ConvertKafkaMessageToTimber(message *sarama.ConsumerMessage) (timber pb.Timber, err error) {
	err = proto.Unmarshal(message.Value, &timber)
	if err != nil {
		err = errkit.Concat(ProtoParseError, err)
		return
	}

	return
}

func ConvertTimberToEsDocumentString(timber pb.Timber, m *jsonpb.Marshaler) (string, error) {
	doc := timber.GetContent()

	if doc == nil {
		return "", TimberContentMissing
	}

	if doc.Fields == nil {
		return "", TimberFieldsMissing
	}

	ts := &stpb.Value{
		Kind: &stpb.Value_StringValue{
			StringValue: timber.GetTimestamp(),
		},
	}
	doc.Fields["@timestamp"] = ts

	docStr, _ := m.MarshalToString(doc)
	return docStr, nil
}
