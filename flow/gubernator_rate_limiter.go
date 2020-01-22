package flow

import (
	"context"
	"net"
	"time"

	"github.com/mailgun/gubernator"
	"github.com/tevino/abool"
	"google.golang.org/grpc"
)

type gubernatorRateLimiter struct {
	address    string
	guber      *gubernator.Instance
	grpcServer *grpc.Server
	isStarted  abool.AtomicBool
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
				Hits:      int64(count),
				Limit:     int64(maxTokenIfNotExist),
				Duration:  (1 * time.Second).Milliseconds(),
			},
		},
	})

	return response.Responses[0].GetStatus() == gubernator.Status_OVER_LIMIT
}

func (limiter *gubernatorRateLimiter) IsStart() bool {
	return limiter.isStarted.IsSet()
}

func (limiter *gubernatorRateLimiter) Start() {
	limiter.isStarted.Set()
	go func() {
		defer limiter.isStarted.UnSet()

		listener, _ := net.Listen("tcp", limiter.address)
		defer listener.Close()

		limiter.grpcServer.Serve(listener)
	}()
}

func (limiter *gubernatorRateLimiter) Stop() {
	limiter.grpcServer.GracefulStop()
}

func (limiter *gubernatorRateLimiter) SetPeers(addresses []string) {
	var peers []gubernator.PeerInfo

	for _, address := range addresses {
		peers = append(peers, gubernator.PeerInfo{
			Address: address,
		})
	}
	limiter.guber.SetPeers(peers)
}
