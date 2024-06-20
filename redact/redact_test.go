package redact

import (
	"regexp"
	"testing"
)

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

func TestJsonPathRule_Redact(t *testing.T) {
	tests := []struct {
		name  string
		want  string
		rule  *JsonPathRule
		input string
	}{
		{
			name:  "Normal email string",
			input: `{"user":{"email":"abc","phone":"123"}}`,
			want:  `{"user":{"email":"[EMAIL REDACTED]","phone":"123"}}`,
			rule: &JsonPathRule{
				Name: "EMAIL",
				Path: regexp.MustCompile(`user\.email`),
			},
		},
		{
			name:  "using a subset of the path",
			input: `{"user":{"email":"abc","phone":"123"}}`,
			want:  `{"user":{"email":"[EMAIL_SHORT REDACTED]","phone":"123"}}`,
			rule: &JsonPathRule{
				Name: "EMAIL_SHORT",
				Path: regexp.MustCompile(`email`),
			},
		},
		{
			name:  "redact email on array of users using full field path",
			input: `{"users":[{"email":"abc","phone":"123"},{"email":"def","phone":"456"}]}`,
			want:  `{"users":[{"email":"[ARRAY_EMAIL REDACTED]","phone":"123"},{"email":"[ARRAY_EMAIL REDACTED]","phone":"456"}]}`,
			rule: &JsonPathRule{
				Name: "ARRAY_EMAIL",
				Path: regexp.MustCompile(`users\[]\.email`),
			},
		},
		{
			name:  "redact email on array of users using short field path",
			input: `{"users":[{"email":"abc","phone":"123"},{"email":"def","phone":"456"}]}`,
			want:  `{"users":[{"email":"[ARRAY_EMAIL_SHORT REDACTED]","phone":"123"},{"email":"[ARRAY_EMAIL_SHORT REDACTED]","phone":"456"}]}`,
			rule: &JsonPathRule{
				Name: "ARRAY_EMAIL_SHORT",
				Path: regexp.MustCompile(`email`),
			},
		},
		{
			name:  "not redact",
			input: `{"user":{"email":"abc","phone":"123"}}`,
			want:  `{"user":{"email":"abc","phone":"123"}}`,
			rule: &JsonPathRule{
				Name: "EMAIL",
				Path: regexp.MustCompile(`user\.mail`),
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
