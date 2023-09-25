package flow

import "github.com/BaritoLog/barito-flow/flow/types"

type consumerOutputFactory struct{}

func NewConsumerOutputFactory() types.ConsumerOutputFactory {
	return &consumerOutputFactory{}
}

func (cf *consumerOutputFactory) MakeConsumerOutputGCS(name string) (types.ConsumerOutput, error) {
	return NewGCSFromEnv(name), nil
}
