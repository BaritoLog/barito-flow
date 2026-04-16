package flow

import "github.com/IBM/sarama"

type fakeClient struct {
	TopicsFunc func() ([]string, error)
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		TopicsFunc: func() ([]string, error) { return []string{}, nil },
	}
}

func (c *fakeClient) Config() *sarama.Config                           { return sarama.NewConfig() }
func (c *fakeClient) Controller() (*sarama.Broker, error)              { return nil, nil }
func (c *fakeClient) RefreshController() (*sarama.Broker, error)       { return nil, nil }
func (c *fakeClient) Brokers() []*sarama.Broker                        { return []*sarama.Broker{} }
func (c *fakeClient) Broker(brokerID int32) (*sarama.Broker, error)    { return nil, nil }
func (c *fakeClient) Topics() ([]string, error)                        { return c.TopicsFunc() }
func (c *fakeClient) Partitions(topic string) ([]int32, error)         { return []int32{}, nil }
func (c *fakeClient) WritablePartitions(topic string) ([]int32, error) { return []int32{}, nil }
func (c *fakeClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	return nil, nil
}
func (c *fakeClient) LeaderAndEpoch(topic string, partitionID int32) (*sarama.Broker, int32, error) {
	return nil, 0, nil
}
func (c *fakeClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	return []int32{}, nil
}
func (c *fakeClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	return []int32{}, nil
}
func (c *fakeClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	return nil, nil
}
func (c *fakeClient) RefreshBrokers(addrs []string) error    { return nil }
func (c *fakeClient) RefreshMetadata(topics ...string) error { return nil }
func (c *fakeClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return 0, nil
}
func (c *fakeClient) Coordinator(consumerGroup string) (*sarama.Broker, error) { return nil, nil }
func (c *fakeClient) RefreshCoordinator(consumerGroup string) error            { return nil }
func (c *fakeClient) TransactionCoordinator(transactionID string) (*sarama.Broker, error) {
	return nil, nil
}
func (c *fakeClient) RefreshTransactionCoordinator(transactionID string) error { return nil }
func (c *fakeClient) InitProducerID() (*sarama.InitProducerIDResponse, error)  { return nil, nil }
func (c *fakeClient) PartitionNotReadable(topic string, partition int32) bool  { return false }
func (c *fakeClient) LeastLoadedBroker() *sarama.Broker                        { return nil }
func (c *fakeClient) Close() error                                             { return nil }
func (c *fakeClient) Closed() bool                                             { return false }
