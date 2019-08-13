package flow

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func onLimitExceededGrpc() error {
	return status.Errorf(codes.ResourceExhausted, "Bandwith Limit Exceeded")
}

func onBadRequestGrpc(err error) error {
	return status.Errorf(codes.InvalidArgument, err.Error())
}

func onStoreErrorGrpc(err error) error {
	return status.Errorf(codes.FailedPrecondition, err.Error())
}

func onCreateTopicErrorGrpc(err error) error {
	return status.Errorf(codes.Unavailable, err.Error())
}

func onSendCreateTopicErrorGrpc(err error) error {
	return status.Errorf(codes.Unavailable, err.Error())
}
