package timber

import "github.com/golang/protobuf/ptypes/any"

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
		Content: map[string]*any.Any{
			"location": &any.Any{Value: []byte(`some-location`)},
			"message":  &any.Any{Value: []byte(`some-message`)},
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
