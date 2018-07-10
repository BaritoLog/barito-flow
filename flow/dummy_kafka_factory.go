package flow

type dummyKafkaFactory struct {
	MakeKafkaAdminFunc     func() (admin KafkaAdmin, err error)
	MakeConsumerWorkerFunc func(groupID, topic string) (worker ConsumerWorker, err error)
}

func NewDummyKafkaFactory() *dummyKafkaFactory {
	return &dummyKafkaFactory{
		MakeKafkaAdminFunc: func() (admin KafkaAdmin, err error) {
			return nil, nil
		},
		MakeConsumerWorkerFunc: func(groupID, topic string) (worker ConsumerWorker, err error) {
			return nil, nil
		},
	}
}

func (f *dummyKafkaFactory) MakeKafkaAdmin() (admin KafkaAdmin, err error) {
	return f.MakeKafkaAdminFunc()
}
func (f *dummyKafkaFactory) MakeConsumerWorker(groupID, topic string) (worker ConsumerWorker, err error) {
	return f.MakeConsumerWorkerFunc(groupID, topic)
}
