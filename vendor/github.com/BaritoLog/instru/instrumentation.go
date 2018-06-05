package instru

import (
	"time"
)

type Instrumentation interface {
	Evaluate(name string) Evaluation
	Count(name string) Counter
	GetEvaluationMetric(name string) *EvaluationMetric
	GetCounterMetric(name string) *CounterMetric
	Flush()
}

type instrumentation struct {
	Evaluations map[string]*EvaluationMetric `json:"evaluations"`
	Counters    map[string]*CounterMetric    `json:"counters"`
}

func NewInstrumentation() Instrumentation {
	return &instrumentation{
		Evaluations: make(map[string]*EvaluationMetric),
		Counters:    make(map[string]*CounterMetric),
	}
}

func (i *instrumentation) Evaluate(name string) Evaluation {
	return NewEvaluation(
		time.Now(),
		i.GetEvaluationMetric(name),
	)
}

func (i *instrumentation) Count(name string) Counter {
	return NewCounter(
		i.GetCounterMetric(name),
	)
}

func (i *instrumentation) GetEvaluationMetric(name string) *EvaluationMetric {
	metric, ok := i.Evaluations[name]
	if !ok {
		metric = NewEvaluationMetric()
		i.Evaluations[name] = metric
	}

	return metric
}

func (i *instrumentation) GetCounterMetric(name string) *CounterMetric {
	metric, ok := i.Counters[name]
	if !ok {
		metric = NewCounterMetric()
		i.Counters[name] = metric
	}
	return metric
}

func (i *instrumentation) Flush() {
	i.Evaluations = make(map[string]*EvaluationMetric)
	i.Counters = make(map[string]*CounterMetric)
}
