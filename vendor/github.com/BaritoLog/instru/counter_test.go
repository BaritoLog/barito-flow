package instru

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestCounter_Event(t *testing.T) {

	metric := NewCounterMetric()
	counter := NewCounter(metric)

	counter.Event("success")
	counter.Event("success")
	counter.Event("success")
	counter.Event("error")
	counter.Event("error")

	FatalIf(t, metric.Total != 5, "wrong metrix.total")
	FatalIf(t, metric.Events["success"] != 3, "wrong metrix.events.success")
	FatalIf(t, metric.Events["error"] != 2, "wrong metrix.events.success")

}
