package instru

import "time"

type Evaluation interface {
	Done()
}

type evaluation struct {
	startTime time.Time
	metric    *EvaluationMetric
}

func NewEvaluation(startTime time.Time, metric *EvaluationMetric) Evaluation {
	return &evaluation{
		startTime: startTime,
		metric:    metric,
	}
}

func (e *evaluation) Done() {
	recent := time.Now().Sub(e.startTime)

	e.metric.Recent = recent
	e.metric.Count++
	e.metric.Sum += recent
	e.metric.Avg = time.Duration(int64(e.metric.Sum) / int64(e.metric.Count))

	if e.metric.Max < recent {
		e.metric.Max = recent
	}

	if e.metric.Min > recent || e.metric.Min <= 0 {
		e.metric.Min = recent
	}
}
