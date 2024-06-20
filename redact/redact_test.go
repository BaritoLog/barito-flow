package redact

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRules_Redact(t *testing.T) {
	emailStaticRule := &StaticRule{
		Name:  "EMAIL",
		Regex: regexp.MustCompile(`[a-z]+@[a-z]+.com`),
	}
	phoneStaticRule := &StaticRule{
		Name:  "PHONE",
		Regex: regexp.MustCompile(`\+6285\d{5,9}`),
	}

	tests := []struct {
		name  string
		input string
		want  string
		rules *Rules
	}{
		{
			name:  "non json log, with 1 match",
			input: "abc user@example.com",
			want:  "abc [EMAIL REDACTED]",
			rules: &Rules{
				StaticRules: []*StaticRule{emailStaticRule, phoneStaticRule},
			},
		},
		{
			name:  "non json log, with 2 match",
			input: "abc user@example.com +628500000000",
			want:  "abc [EMAIL REDACTED] [PHONE REDACTED]",
			rules: &Rules{
				StaticRules: []*StaticRule{emailStaticRule, phoneStaticRule},
			},
		},
		{
			name: "json log, with 1 match",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.rules.Redact(tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}
func TestStaticRule_Redact(t *testing.T) {
	emailRegex := regexp.MustCompile(`[a-z]+@[a-z]+.com`)
	tests := []struct {
		name  string
		want  string
		rule  *StaticRule
		input string
	}{
		{
			name:  "Normal email string",
			input: `{"email":"user@example.com"}`,
			want:  `{"email":"[EMAIL REDACTED]"}`,
			rule: &StaticRule{
				Name:  "EMAIL",
				Regex: emailRegex,
			},
		},
		{
			name:  "not match",
			input: `{"email":"something"}`,
			want:  `{"email":"something"}`,
			rule: &StaticRule{
				Name:  "EMAIL",
				Regex: emailRegex,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.rule.Redact(tc.input)
			if got != tc.want {
				t.Fatalf("expected: %v, got: %v", tc.want, got)
			}
		})
	}
}
