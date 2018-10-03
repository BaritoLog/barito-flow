package flow

import (
	"encoding/json"
	"net/http"
)

type ProduceResult struct {
	Topic      string `json:"topic"`
	IsNewTopic bool   `json:"is_new_topic"`
}

func onLimitExceeded(rw http.ResponseWriter) {
	rw.WriteHeader(509)
	rw.Write([]byte("Bandwith Limit Exceeded"))
}

func onBadRequest(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusBadRequest)
	rw.Write([]byte(err.Error()))
}

func onStoreError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusBadGateway)
	rw.Write([]byte(err.Error()))
}

func onSuccess(rw http.ResponseWriter, result ProduceResult) {
	rw.WriteHeader(http.StatusOK)

	b, _ := json.Marshal(result)
	rw.Write(b)
}

func onCreateTopicError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusServiceUnavailable)
	rw.Write([]byte(err.Error()))
}

func onSendCreateTopicError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusServiceUnavailable)
	rw.Write([]byte(err.Error()))
}
