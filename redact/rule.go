package redact

import (
	"encoding/json"
	"fmt"
)

func (r *StaticRule) Redact(s string) string {
	return r.Regex.ReplaceAllStringFunc(s, func(match string) string {
		if r.HintCharsStart > 0 {
			if r.HintCharsEnd > 0 {
				return fmt.Sprintf("%s[%s REDACTED]%s", match[:r.HintCharsStart], r.Name, match[len(match)-r.HintCharsEnd:])
			}
			return fmt.Sprintf("%s[%s REDACTED]", match[:r.HintCharsStart], r.Name)
		}
		if r.HintCharsEnd > 0 {
			return fmt.Sprintf("[%s REDACTED]%s", r.Name, match[len(match)-r.HintCharsEnd:])
		}
		return fmt.Sprintf("[%s REDACTED]", r.Name)
	})
}

func (r *JsonPathRule) Redact(s string) string {
	if r.HintCharsStart > 0 {
		if r.HintCharsEnd > 0 {
			return fmt.Sprintf("%s[%s REDACTED]%s", s[:r.HintCharsStart], r.Name, s[len(s)-r.HintCharsEnd:])
		}
		return fmt.Sprintf("%s[%s REDACTED]", s[:r.HintCharsStart], r.Name)
	}
	if r.HintCharsEnd > 0 {
		return fmt.Sprintf("[%s REDACTED]%s", r.Name, s[len(s)-r.HintCharsEnd:])
	}

	return fmt.Sprintf("[%s REDACTED]", r.Name)
}

func (r *Rules) Redact(originalString string) string {
	var s = originalString
	var data interface{}

	if len(r.JsonPathRules) != 0 {
		if err := json.Unmarshal([]byte(s), &data); err == nil {
			modifiedData := r.traverseJSON(data, "")
			modifiedJSON, err := json.Marshal(modifiedData)
			if err != nil {
				// TODO: log error
			} else {
				s = string(modifiedJSON)
			}
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
		if r.JsonPathRules == nil {
			return v
		}

		for _, rule := range r.JsonPathRules {
			if rule.Path.MatchString(path) {
				return rule.Redact(v)
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
