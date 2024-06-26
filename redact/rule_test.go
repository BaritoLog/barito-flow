package redact

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRules_Redact(t *testing.T) {
	emailStaticRule := &StaticRule{
		Name:  "EMAIL",
		Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+.com`)},
	}
	phoneStaticRule := &StaticRule{
		Name:  "PHONE",
		Regex: &Regexp{Regexp: regexp.MustCompile(`\+6285\d{5,9}`)},
	}
	emailJsonPathRule := &JsonPathRule{
		Name: "EMAIL",
		Path: &Regexp{Regexp: regexp.MustCompile("users.details.email")},
	}
	phoneJsonPathRule := &JsonPathRule{
		Name: "PHONE",
		Path: &Regexp{Regexp: regexp.MustCompile("users.details.phone")},
	}

	tests := []struct {
		name  string
		input string
		want  string
		rules *Rules
	}{
		{
			name:  "non json log, with 1 match and static rules",
			input: "abc user@example.com",
			want:  "abc [EMAIL REDACTED]",
			rules: &Rules{
				StaticRules: []*StaticRule{emailStaticRule, phoneStaticRule},
			},
		},
		{
			name:  "non json log, with 2 match and static rules",
			input: "abc user@example.com +628500000000",
			want:  "abc [EMAIL REDACTED] [PHONE REDACTED]",
			rules: &Rules{
				StaticRules: []*StaticRule{emailStaticRule, phoneStaticRule},
			},
		},
		{
			name:  "json log, with 1 match and static rules",
			input: `{"email":"user@example.com"}`,
			want:  `{"email":"[EMAIL REDACTED]"}`,
			rules: &Rules{
				StaticRules: []*StaticRule{emailStaticRule, phoneStaticRule},
			},
		},
		{
			name:  "json log, with 2 match and static rules",
			input: `{"email":"user@example.com","phone":"+6285848484844"}`,
			want:  `{"email":"[EMAIL REDACTED]","phone":"[PHONE REDACTED]"}`,
			rules: &Rules{
				StaticRules: []*StaticRule{emailStaticRule, phoneStaticRule},
			},
		},
		{
			name:  "nested json log, with 2 match and static rules",
			input: `{"users":{"details":{"email":"test@gmail.com","phone":"+6285848484844"}}}`,
			want:  `{"users":{"details":{"email":"[EMAIL REDACTED]","phone":"[PHONE REDACTED]"}}}`,
			rules: &Rules{
				StaticRules: []*StaticRule{emailStaticRule, phoneStaticRule},
			},
		},
		{
			name:  "nested json log, with 1 match and jsonpath rules",
			input: `{"users":{"details":{"email":"test@gmail.com","phone":"+6285848484844"}}}`,
			want:  `{"users":{"details":{"email":"[EMAIL REDACTED]","phone":"+6285848484844"}}}`,
			rules: &Rules{
				JsonPathRules: []*JsonPathRule{emailJsonPathRule},
			},
		},
		{
			name:  "nested json log, with 2 match and jsonpath rules",
			input: `{"users":{"details":{"email":"test@gmail.com","phone":"+6285848484844"}}}`,
			want:  `{"users":{"details":{"email":"[EMAIL REDACTED]","phone":"[PHONE REDACTED]"}}}`,
			rules: &Rules{
				JsonPathRules: []*JsonPathRule{emailJsonPathRule, phoneJsonPathRule},
			},
		},
		{
			name:  "nested json log containing array, with 2 match and jsonpath rules",
			input: `{"users":{"details":[{"email":"test@gmail.com","phone":"+6285848484844"},{"email":"test1@gmail.com","phone":"+6285848484864"}]}}`,
			want:  `{"users":{"details":[{"email":"[EMAIL REDACTED]","phone":"[PHONE REDACTED]"},{"email":"[EMAIL REDACTED]","phone":"[PHONE REDACTED]"}]}}`,
			rules: &Rules{
				JsonPathRules: []*JsonPathRule{
					&JsonPathRule{
						Name: "EMAIL",
						Path: &Regexp{Regexp: regexp.MustCompile(`users.details\[]\.email`)},
					},
					&JsonPathRule{
						Name: "PHONE",
						Path: &Regexp{Regexp: regexp.MustCompile(`users.details\[]\.phone`)},
					},
				},
			},
		},
		{
			name:  "nested json log containing array, with jsonpath and static rules",
			input: `{"users":{"details":[{"email":"test@gmail.com","phone":"+6285848484844"},{"email":"test1@gmail.com","phone":"+6285848484864"}]}}`,
			want:  `{"users":{"details":[{"email":"[EMAIL REDACTED]","phone":"[PHONE REDACTED]"},{"email":"[EMAIL REDACTED]","phone":"[PHONE REDACTED]"}]}}`,
			rules: &Rules{
				JsonPathRules: []*JsonPathRule{
					&JsonPathRule{
						Name: "EMAIL",
						Path: &Regexp{Regexp: regexp.MustCompile(`users.details\[]\.email`)},
					},
				},
				StaticRules: []*StaticRule{phoneStaticRule},
			},
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
	emailRegex := &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+.com`)}
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
