package flow

import (
	"encoding/json"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/Shopify/sarama"
	pb "github.com/bentol/barito-proto/producer"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	stpb "github.com/golang/protobuf/ptypes/struct"
)

const (
	JsonParseError       = errkit.Error("JSON Parse Error")
	ProtoParseError      = errkit.Error("Protobuf Parse Error")
	TimberContentMissing = errkit.Error("Timber Content Missing Error")
	TimberFieldsMissing  = errkit.Error("Timber Field Missing Error")
)

type LogFormatGcsSimpler struct {
	Message interface{} `json:"message"`
	LogTag  string      `json:"log_tag"`
	LogTime string      `json:"log_time"`
}

func ConvertTimberToKafkaMessage(timber *pb.Timber, topic string) *sarama.ProducerMessage {
	b, _ := proto.Marshal(timber)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
}

func ConvertTimberCollectionToKafkaMessage(timberCollection *pb.TimberCollection, topic string) *sarama.ProducerMessage {
	b, _ := proto.Marshal(timberCollection)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte(TimberCollectionMessageFormat),
				Value: sarama.ByteEncoder("true"),
			},
		},
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

func ConvertKafkaMessageToTimberCollection(message *sarama.ConsumerMessage) (timberCollection pb.TimberCollection, err error) {
	err = proto.Unmarshal(message.Value, &timberCollection)
	if err != nil {
		err = errkit.Concat(ProtoParseError, err)
		return
	}

	return
}

func ConvertTimberToLogFormatGCSSimpleString(timber pb.Timber) (string, error) {
	doc := timber.GetContent()

	if doc == nil {
		return "", TimberContentMissing
	}

	if doc.Fields == nil {
		return "", TimberFieldsMissing
	}

	metadata, ok := doc.Fields["k8s_metadata"]
	if !ok || metadata == nil {
		return "", TimberFieldsMissing
	}

	containerName, ok := metadata.GetStructValue().Fields["container_name"]
	if !ok || containerName == nil {
		return "", TimberFieldsMissing
	}

	obj := &LogFormatGcsSimpler{
		LogTag:  containerName.GetStringValue(),
		LogTime: doc.Fields["@timestamp"].GetStringValue(),
	}

	messageString := doc.Fields["@message"].GetStringValue()
	if len(messageString) == 0 {
		return "", TimberFieldsMissing
	}
	if messageString[0] == '{' {
		o := map[string]interface{}{}
		err := json.Unmarshal([]byte(messageString), &o)
		if err != nil {
			return "", err
		}
		obj.Message = o
	} else if messageString[0] == '[' {
		o := []interface{}{}
		err := json.Unmarshal([]byte(messageString), &o)
		if err != nil {
			return "", err
		}
		obj.Message = o
	} else {
		obj.Message = messageString
	}

	objStr, err := json.Marshal(obj)
	return string(objStr), err

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
