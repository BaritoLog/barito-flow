package instru

import "time"

type EvaluationMetric struct {
	Count  int32         `json:"count"`
	Avg    time.Duration `json:"avg"`
	Sum    time.Duration `json:"sum"`
	Max    time.Duration `json:"max"`
	Min    time.Duration `json:"min"`
	Recent time.Duration `json:"recent"`
}

func NewEvaluationMetric() *EvaluationMetric {
	return &EvaluationMetric{}
}
