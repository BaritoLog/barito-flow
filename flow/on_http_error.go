package flow

import "net/http"

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

func onSuccess(rw http.ResponseWriter) {
	rw.WriteHeader(http.StatusOK)
	rw.Write([]byte("OK"))
}

func onCreateTopicError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(http.StatusServiceUnavailable)
	rw.Write([]byte(err.Error()))
}
