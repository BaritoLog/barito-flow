package flow

type dummyKafkaFactory struct {
	MakeKafkaAdminFunc      func() (admin KafkaAdmin, err error)
	MakeClusterConsumerFunc func(groupID, topic string) (consumer ClusterConsumer, err error)
}

func NewDummyKafkaFactory() *dummyKafkaFactory {
	return &dummyKafkaFactory{
		MakeKafkaAdminFunc: func() (admin KafkaAdmin, err error) {
			return nil, nil
		},
		MakeClusterConsumerFunc: func(groupID, topic string) (worker ClusterConsumer, err error) {
			return nil, nil
		},
	}
}

func (f *dummyKafkaFactory) MakeKafkaAdmin() (admin KafkaAdmin, err error) {
	return f.MakeKafkaAdminFunc()
}
func (f *dummyKafkaFactory) MakeClusterConsumer(groupID, topic string) (worker ClusterConsumer, err error) {
	return f.MakeClusterConsumerFunc(groupID, topic)
}
