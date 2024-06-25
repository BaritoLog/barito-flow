package redact

import (
	"encoding/json"
)

type Redactor struct {
	RulesMap map[string]Rules `json:"rules"`
}

func (r *Redactor) Redact(appName, doc string) (redactedDoc string, err error) {
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

func NewRedactorFromJSON(jsonRulesMap string) (redactor *Redactor, err error) {
	rulesMap := make(map[string]Rules)
	json.Unmarshal([]byte(jsonRulesMap), &rulesMap)

	redactor = &Redactor{
		RulesMap: rulesMap,
	}
	return redactor, nil
}
