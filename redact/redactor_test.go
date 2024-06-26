package redact

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRedactorFromJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Redactor
		wantErr bool
		errMsg  string
	}{
		{
			name:  "valid json",
			input: `{"default":{"StaticRules":[{"Name":"EMAIL","Regex":"[a-z]+@[a-z]+\\.com"}],"JsonPathRules":[{"Name":"EMAIL","Path":"email"}]},"istio":{"StaticRules":[{"Name":"auth-header","Regex":"Bearer\\s+[\\w-]+"}]}}`,
			want: &Redactor{
				RulesMap: map[string]Rules{
					"default": {
						StaticRules: []*StaticRule{
							{
								Name:  "EMAI",
								Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+\.com`)},
							},
						},
						JsonPathRules: []*JsonPathRule{
							{
								Name: "EMAIL",
								Path: &Regexp{Regexp: regexp.MustCompile("email")},
							},
						},
					},
					"istio": {
						StaticRules: []*StaticRule{
							{
								Name:  "auth-header",
								Regex: &Regexp{Regexp: regexp.MustCompile(`Bearer\s+[\w-]+`)},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewRedactorFromJSON(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
				fmt.Println(got.RulesMap["default"].StaticRules[0].Name)
				fmt.Println(got.RulesMap["default"].StaticRules[0].Regex)
				fmt.Println(got.RulesMap["default"].JsonPathRules[0].Name)
				fmt.Println(got.RulesMap["default"].JsonPathRules[0].Path)
			}
		})
	}
}

func TestRedactor_Redact(t *testing.T) {
	redactor := &Redactor{
		RulesMap: map[string]Rules{
			"default": {
				StaticRules: []*StaticRule{
					{
						Name:  "EMAIL",
						Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+\.com`)},
					},
				},
				JsonPathRules: []*JsonPathRule{
					{
						Name: "EMAIL",
						Path: &Regexp{Regexp: regexp.MustCompile("email")},
					},
				},
			},
			"istio": {
				StaticRules: []*StaticRule{
					{
						Name:  "auth-header",
						Regex: &Regexp{Regexp: regexp.MustCompile(`Bearer\s+[\w-]+`)},
					},
				},
			},
		},
	}

	tests := []struct {
		name    string
		appName string
		doc     string
		want    string
	}{
		{
			name:    "redact email",
			appName: "user-services",
			doc:     `{"email":"example.com"}`,
			want:    `{"email":"[EMAIL REDACTED]"}`,
		},
		{
			name:    "not redact email, different app",
			appName: "istio",
			doc:     `{"email":"example.com"}`,
			want:    `{"email":"example.com"}`,
		},
		{
			name:    "redact auth header, use different app",
			appName: "istio",
			doc:     `{"auth":"Bearer token"}`,
			want:    `{"auth":"[auth-header REDACTED]"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := redactor.Redact(tt.appName, tt.doc)
			assert.Equal(t, tt.want, got)
		})
	}
}
