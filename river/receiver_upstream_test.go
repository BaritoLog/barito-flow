package river

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestReceiverUpstream_New(t *testing.T) {
	upstream, err := NewReceiverUpstream(ReceiverUpstreamConfig{Addr: ":8080"})
	FatalIfError(t, err)

	recevier, ok := upstream.(*receiverUpstream)
	FatalIf(t, !ok, "upstream must be ReceiverUpstream")
	FatalIf(t, recevier.addr == "", "must setup addr in receiver")
	FatalIf(t, recevier.errCh == nil, "must setup error channel in receiver")
	FatalIf(t, recevier.timberCh == nil, "must setup timber channel in receiver")
}

func TestReceiverUpstream_New_WrongParameter(t *testing.T) {
	_, err := NewReceiverUpstream("meh")
	FatalIfWrongError(t, err, "Parameter must be ReceiverUpstreamConfig")
}

// func TestReceiverUpstream_ProduceHandler_Success(t *testing.T) {
// 	upstream := &receiverUpstream{
// 		addr:     ":8080",
// 		timberCh: make(chan Timber),
// 		errCh:    make(chan error),
// 	}
//
// 	// submit to /produce
// 	req, _ := http.NewRequest(
// 		"POST",
// 		"/str/18/st/1/fw/1/cl/10/produce/kafka-dummy-topic",
// 		strings.NewReader("some log"),
// 	)
// 	rec := httptest.NewRecorder()
//
// 	go upstream.router().ServeHTTP(rec, req)
// 	timekit.Sleep("1ms")
//
// 	FatalIfWrongHttpCode(t, rec, http.StatusOK)
//
// 	timber := <-upstream.TimberChannel()
// 	loc := timber.Location
// 	data := string(timber.Message)
// 	FatalIf(t, loc != "kafka-dummy-topic", "wrong location: %s", loc)
// 	FatalIf(t, data != "some log", "wrong location: %s", data)
// }

// func TestReceiverUpstream_SetErrorChannel(t *testing.T) {
// 	errCh := make(chan error)
// 	receiver := receiverUpstream{}
//
// 	receiver.SetErrorChannel(errCh)
// 	FatalIf(t, errCh != receiver.ErrorChannel(), "SetErrorChannel is not working")
//
// }
