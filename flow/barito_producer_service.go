package flow

import (
	"net/http"

	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
)

type BaritoProducerService interface {
	Start() error
	Close()
	ServeHTTP(rw http.ResponseWriter, req *http.Request)
}

type baritoProducerService struct {
	Producer    sarama.SyncProducer
	TopicSuffix string

	bucket LeakyBucket
	server *http.Server
}

func NewBaritoProducerService(addr string, producer sarama.SyncProducer, maxTps int, topicSuffix string) BaritoProducerService {
	s := &baritoProducerService{
		Producer:    producer,
		TopicSuffix: topicSuffix,
		bucket:      NewLeakyBucket(maxTps, timekit.Duration("1s")),
	}
	s.server = &http.Server{
		Addr:    addr,
		Handler: s,
	}

	return s
}

func (a *baritoProducerService) Start() error {

	a.bucket.StartRefill()

	return a.server.ListenAndServe()
}

func (a *baritoProducerService) Close() {
	if a.server != nil {
		a.server.Close()
	}

	if a.bucket != nil {
		a.bucket.Close()
	}

	a.Producer.Close()
}

func (s *baritoProducerService) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if !s.bucket.Take() {
		onLimitExceeded(rw)
		return
	}
	timber, err := ConvertRequestToTimber(req)
	if err != nil {
		onBadRequest(rw, err)
		return
	}

	err = kafkaStore(s.Producer, timber, s.TopicSuffix)
	if err != nil {
		onStoreError(rw, err)
		return
	}

	onSuccess(rw)
}
