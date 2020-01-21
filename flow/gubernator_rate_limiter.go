package flow

import (
	"context"
	"net"
	"time"

	"github.com/mailgun/gubernator"
	"google.golang.org/grpc"
)

type gubernatorRateLimiter struct {
	address    string
	guber      *gubernator.Instance
	grpcServer *grpc.Server
}

func newGubernatorRateLimiter(grpcAddress string) *gubernatorRateLimiter {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024 * 1024),
	)
	guber, _ := gubernator.New(gubernator.Config{
		GRPCServer: grpcServer,
		Cache:      gubernator.NewLRUCache(1024),
	})

	return &gubernatorRateLimiter{
		address:    grpcAddress,
		guber:      guber,
		grpcServer: grpcServer,
	}
}

func (limiter *gubernatorRateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	response, _ := limiter.guber.GetRateLimits(context.Background(), &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			&gubernator.RateLimitReq{
				Name:      "tps",
				UniqueKey: topic,
				Hits:      1,
				Limit:     int64(10),
				Duration:  (1 * time.Second).Milliseconds(),
			},
		},
	})

	return response.Responses[0].GetStatus() == gubernator.Status_OVER_LIMIT
}

func (*gubernatorRateLimiter) IsStart() bool {
	return false
}

func (limiter *gubernatorRateLimiter) Start() {
	go func() {
		listener, _ := net.Listen("tcp", limiter.address)
		limiter.grpcServer.Serve(listener)
	}()
}

func (*gubernatorRateLimiter) Stop() {}

func (limiter *gubernatorRateLimiter) SetPeers(addresses []string) {
	var peers []gubernator.PeerInfo

	for _, address := range addresses {
		peers = append(peers, gubernator.PeerInfo{
			Address: address,
		})
	}
	limiter.guber.SetPeers(peers)
}
