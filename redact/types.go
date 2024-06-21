package redact

import "regexp"

type Rules struct {
	StaticRules   []*StaticRule
	JsonPathRules []*JsonPathRule
}

type StaticRule struct {
	Name  string
	Regex *regexp.Regexp
}

type JsonPathRule struct {
	Name string
	Path *regexp.Regexp
}
