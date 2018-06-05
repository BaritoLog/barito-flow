package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/BaritoLog/instru"
)

type MetricMarketCallback interface {
	OnCallback(instr instru.Instrumentation) error
}

type metricMarketCallback struct {
	url   string
	token string
}

func NewMetricMarketCallback(url, token string) MetricMarketCallback {
	return &metricMarketCallback{url, token}
}

func (c *metricMarketCallback) OnCallback(instr instru.Instrumentation) (err error) {
	count := instru.GetEventCount("es_store", "success")
	contract := map[string]interface{}{
		"new_log_count": count,
		"token":         c.token,
	}

	b, _ := json.Marshal(contract)

	resp, err := http.Post(c.url, "application/json", bytes.NewReader(b))
	if err != nil {
		return
	}

	if resp.StatusCode != 200 {
		err = fmt.Errorf("Got status code %d", resp.StatusCode)
	}

	instr.Flush()
	return
}
