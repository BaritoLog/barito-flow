package redact

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
)

type Rule interface {
	Redact(string) string
}

type StaticRule struct {
	Name  string
	Regex *regexp.Regexp
}

func (r *StaticRule) Redact(s string) string {
	return r.Regex.ReplaceAllString(s, fmt.Sprintf("[%s REDACTED]", r.Name))
}

type JsonPathRule struct {
	Name string
	Path *regexp.Regexp
}

func (r *JsonPathRule) traverseJSON(data interface{}, path string) interface{} {
	switch v := data.(type) {
	case string:
		if r.Path.MatchString(path) {
			return fmt.Sprintf("[%s REDACTED]", r.Name)
		}
		return v
	case float64:
		return v
	case map[string]interface{}:
		for key, value := range v {
			v[key] = r.traverseJSON(value, fmt.Sprintf("%s.%s", path, key))
		}
		return v
	case []interface{}:
		for i, value := range v {
			v[i] = r.traverseJSON(value, fmt.Sprintf("%s[]", path))
		}
		return v
	default:
		return v
	}
}

func (r *JsonPathRule) Redact(s string) string {
	var data interface{}

	if err := json.Unmarshal([]byte(s), &data); err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	modifiedData := r.traverseJSON(data, "")
	modifiedJSON, err := json.Marshal(modifiedData)
	if err != nil {
		log.Fatalf("Error marshalling modified JSON: %v", err)
	}

	return string(modifiedJSON)
}
