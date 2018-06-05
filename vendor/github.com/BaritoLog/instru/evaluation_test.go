package instru

import (
	"testing"
	"time"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
)

func TestEvaluation_Done(t *testing.T) {
	timekit.FreezeUTC("2017-10-02T10:00:00-05:00")
	defer timekit.Unfreeze()

	metric := NewEvaluationMetric()

	NewEvaluation(
		time.Now().Add(-timekit.Duration("1s")),
		metric,
	).Done()

	FatalIf(t, metric.Recent.String() != "1s", "wrong sum")
	FatalIf(t, metric.Sum.String() != "1s", "wrong sum")
	FatalIf(t, metric.Avg.String() != "1s", "wrong avg")
	FatalIf(t, metric.Min.String() != "1s", "wrong min")
	FatalIf(t, metric.Max.String() != "1s", "wrong max")

	NewEvaluation(
		time.Now().Add(-timekit.Duration("3s")),
		metric,
	).Done()

	FatalIf(t, metric.Recent.String() != "3s", "wrong sum")
	FatalIf(t, metric.Sum.String() != "4s", "wrong sum")
	FatalIf(t, metric.Avg.String() != "2s", "wrong avg")
	FatalIf(t, metric.Min.String() != "1s", "wrong min")
	FatalIf(t, metric.Max.String() != "3s", "wrong max")

}
