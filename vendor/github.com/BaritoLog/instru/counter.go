package instru

type Counter interface {
	Event(name string)
}

type counter struct {
	metric *CounterMetric
}

func NewCounter(metric *CounterMetric) Counter {
	return &counter{
		metric: metric,
	}
}

func (s *counter) Event(name string) {
	s.metric.Total++
	s.metric.Events[name]++
}
