package flow

import (
	"encoding/json"
	"net/http"

	log "github.com/sirupsen/logrus"
)

type ProduceResult struct {
	Topic string `json:"topic"`
}

func onLimitExceeded(rw http.ResponseWriter) {
	rw.WriteHeader(509)
	rw.Write([]byte("Bandwith Limit Exceeded"))
}

func onBadRequest(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusBadRequest)
	rw.Write([]byte(err.Error()))
	log.Warn(err)
}

func onStoreError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusBadGateway)
	rw.Write([]byte(err.Error()))
	log.Warn(err)
}

func onSuccess(rw http.ResponseWriter, result ProduceResult) {
	rw.WriteHeader(http.StatusOK)

	b, _ := json.Marshal(result)
	rw.Write(b)
}

func onCreateTopicError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusServiceUnavailable)
	rw.Write([]byte(err.Error()))
	log.Warn(err)
}

func onSendCreateTopicError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusServiceUnavailable)
	rw.Write([]byte(err.Error()))
	log.Warn(err)
}
