package instru

import (
	"time"
)

var DefaultInstrumentation Instrumentation = NewInstrumentation()
var DefaultExposer Exposer
var DefaultCallback Callback

var ErrorCh = make(chan error)
var OnErrorFunc func(error)

var CallbackStop chan int

func init() {
	go loopError()
}

func Evaluate(name string) Evaluation {
	return DefaultInstrumentation.Evaluate(name)
}

func Count(name string) Counter {
	return DefaultInstrumentation.Count(name)
}

func Expose(exposer Exposer) {
	go func() {
		ErrorCh <- exposer.Expose(DefaultInstrumentation)
	}()
	DefaultExposer = exposer
}

func ExposeWithRestful(addr string) {
	Expose(NewRestfulExposer(addr))
}

func StopExpose() {
	if DefaultExposer != nil {
		DefaultExposer.Stop()
	}
	DefaultExposer = nil
}

func SetCallback(interval time.Duration, callback Callback) {
	tick := time.Tick(interval)

	DefaultCallback = callback
	CallbackStop = make(chan int)

	go func() {
		for {
			select {
			case <-tick:
				err := callback.OnCallback(DefaultInstrumentation)
				fireError(err)
			case <-CallbackStop:
				return
			}
		}
	}()
}

func SetWebCallback(interval time.Duration, url string) {
	SetCallback(interval, NewWebCallback(url))
}

func UnsetCallback() {
	if CallbackStop != nil {
		CallbackStop <- 1
	}

	CallbackStop = nil
	DefaultCallback = nil
}

func GetEventCount(label, event string) int64 {
	counter := DefaultInstrumentation.GetCounterMetric(label)
	return counter.TotalEvent(event)
}

func Flush() {
	DefaultInstrumentation.Flush()
}

func loopError() {
	for err := range ErrorCh {
		fireError(err)
	}
}

func fireError(err error) {
	if OnErrorFunc != nil {
		OnErrorFunc(err)
	}
}
