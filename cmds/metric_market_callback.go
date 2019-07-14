package cmds

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/instru"
)

type MetricPayload struct {
	ApplicationGroups []ApplicationGroup `json:"application_groups"`
}

type ApplicationGroup struct {
	AppSecret string `json:"token"`
	LogCount  int64  `json:"new_log_count"`
}

// TODO: move to flow package
type MetricMarketCallback interface {
	OnCallback(instr instru.Instrumentation) error
}

type metricMarketCallback struct {
	url string
}

func NewMetricMarketCallback(url string) MetricMarketCallback {
	return &metricMarketCallback{url}
}

func (c *metricMarketCallback) OnCallback(instr instru.Instrumentation) (err error) {

	metricPayload := MetricPayload{}
	var application_groups []ApplicationGroup

	collection := flow.GetApplicationSecretCollection()

	if len(collection) == 0 {
		application_groups = []ApplicationGroup{}
	}

	for _, appSecret := range collection {
		count := instru.GetEventCount(fmt.Sprintf("%s_es_store", appSecret), "success")
		application_groups = append(application_groups, ApplicationGroup{
			AppSecret: appSecret,
			LogCount:  count,
		})
	}

	metricPayload.ApplicationGroups = application_groups

	b, _ := json.Marshal(metricPayload)

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
