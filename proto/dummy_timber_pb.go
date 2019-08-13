package timber

import "github.com/golang/protobuf/ptypes/struct"

func SampleTimberContextProto() *TimberContext {
	return &TimberContext{
		KafkaTopic:             "some_topic",
		KafkaPartition:         3,
		KafkaReplicationFactor: 1,
		EsIndexPrefix:          "some-type",
		EsDocumentType:         "some-type",
		AppMaxTps:              10,
		AppSecret:              "some-secret-1234",
	}
}

func SampleTimberProto() *Timber {
	return &Timber{
		Context: SampleTimberContextProto(),
		Content: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"location": &structpb.Value{
					Kind: &structpb.Value_StringValue{
						StringValue: "some-location",
					},
				},
				"message": &structpb.Value{
					Kind: &structpb.Value_StringValue{
						StringValue: "some-message",
					},
				},
			},
		},
	}
}

func SampleTimberCollectionProto() *TimberCollection {
	return &TimberCollection{
		Context: SampleTimberContextProto(),
		Items: []*Timber{
			SampleTimberProto(),
			SampleTimberProto(),
		},
	}
}
