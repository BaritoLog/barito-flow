package instru

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestTotalEvent(t *testing.T) {
	metric := CounterMetric{
		Total: 10,
		Events: map[string]int64{
			"some-event":  8,
			"other-event": 2,
		},
	}

	FatalIf(t, metric.TotalEvent("some-event") != 8, "wrong total some-event")
	FatalIf(t, metric.TotalEvent("wrong-event") != 0, "wrong total wrong-event")
}
