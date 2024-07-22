package redact

import (
	"encoding/json"
	"regexp"
)

type Rules struct {
	StaticRules   []*StaticRule
	JsonPathRules []*JsonPathRule
}

type Regexp struct {
	*regexp.Regexp
}

func (r *Regexp) UnmarshalJSON(b []byte) error {
	var pattern string
	if err := json.Unmarshal(b, &pattern); err != nil {
		return err
	}
	re := regexp.MustCompile(pattern)
	r.Regexp = re
	return nil
}

type StaticRule struct {
	Name           string
	Regex          *Regexp
	HintCharsStart int
	HintCharsEnd   int
}

type JsonPathRule struct {
	Name           string
	Path           *Regexp
	HintCharsStart int
	HintCharsEnd   int
}
