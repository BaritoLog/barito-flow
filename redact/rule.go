package redact

import (
	"encoding/json"
	"fmt"
)

func (r *StaticRule) Redact(s string) string {
	return r.Regex.ReplaceAllString(s, fmt.Sprintf("[%s REDACTED]", r.Name))
}

func (r *Rules) Redact(originalString string) string {
	var s = originalString
	var data interface{}

	if err := json.Unmarshal([]byte(s), &data); err == nil {
		modifiedData := r.traverseJSON(data, "")
		modifiedJSON, err := json.Marshal(modifiedData)
		if err != nil {
			// TODO: log error
		} else {
			s = string(modifiedJSON)
		}
	}

	// continue to static rules
	for _, rule := range r.StaticRules {
		s = rule.Redact(s)
	}
	return string(s)
}

func (r *Rules) traverseJSON(data interface{}, path string) interface{} {
	switch v := data.(type) {
	case string:
		for _, rule := range r.JsonPathRules {
			if rule.Path.MatchString(path) {
				return fmt.Sprintf("[%s REDACTED]", rule.Name)
			}
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
