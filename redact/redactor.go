package redact

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Redactor struct {
	RulesMap map[string]Rules `json:"rules"`
	lock     sync.Mutex
}

func (r *Redactor) Redact(appName, doc string) (redactedDoc string, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if rules, ok := r.RulesMap[appName]; ok {
		return rules.Redact(doc), nil
	}

	rules := r.RulesMap["default"]
	return rules.Redact(doc), nil
}

func (r *Redactor) ToJson() (string, error) {
	jsonBytes, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func (r *Redactor) UpdateRulesMap(newRulesMap map[string]Rules) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.RulesMap = newRulesMap
}

func NewRedactorFromJSON(jsonRulesMap string) (redactor *Redactor, err error) {
	rulesMap := make(map[string]Rules)
	json.Unmarshal([]byte(jsonRulesMap), &rulesMap)

	redactor = &Redactor{
		RulesMap: rulesMap,
	}

	fmt.Println("New redactor created with rules: ")
	for name, rules := range rulesMap {
		fmt.Println(name, rules)
	}
	return redactor, nil
}

func NewRedactorFromMarket(marketEndpoint, clusterName string) (redactor *Redactor, err error) {
	rulesMap, err := fetchRulesMapFromMarket(marketEndpoint, clusterName)
	if err != nil {
		panic(err)
	}

	redactor = &Redactor{
		RulesMap: rulesMap,
	}

	go func() {
		for {
			time.Sleep(time.Minute)
			rulesMap, err := fetchRulesMapFromMarket(marketEndpoint, clusterName)
			if err != nil {
				fmt.Println("Failed to fetch the rules", marketEndpoint, clusterName)
				continue
			}

			fmt.Println("Got rulesmap", len(rulesMap))
			redactor.UpdateRulesMap(rulesMap)
		}
	}()

	return
}

func fetchRulesMapFromMarket(marketEndpoint, clusterName string) (rulesMap map[string]Rules, err error) {
	rulesMap = make(map[string]Rules)

	url := fmt.Sprintf("%s?cluster_name=%s", marketEndpoint, clusterName)
	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error get the rules")
		return
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Printf("Error reading response body: %v\n", err)
		return
	}

	err = json.Unmarshal(body, &rulesMap)
	if err != nil {
		fmt.Printf("Error unmarshalling response: %v\n", err)
		return
	}

	return
}
