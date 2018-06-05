package instru

type CounterMetric struct {
	Total  int64
	Events map[string]int64
}

func NewCounterMetric() *CounterMetric {
	return &CounterMetric{
		Events: make(map[string]int64),
	}
}

func (m *CounterMetric) TotalEvent(name string) int64 {
	return m.Events[name]
}
