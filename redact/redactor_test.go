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
func BenchmarkRedactor_Redact(b *testing.B) {
	redactor := &Redactor{
		RulesMap: map[string]Rules{
			"default": {
				StaticRules: []*StaticRule{
					{Name: "EMAIL", Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+\.com`)}},
				},
			},
			"multi-static": {
				StaticRules: []*StaticRule{
					{Name: "EMAIL1", Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+\.com`)}},
					{Name: "EMAIL2", Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+\.com`)}},
					{Name: "EMAIL3", Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+\.com`)}},
					{Name: "EMAIL4", Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+\.com`)}},
					{Name: "EMAIL5", Regex: &Regexp{Regexp: regexp.MustCompile(`[a-z]+@[a-z]+\.com`)}},
				},
			},
			"json-rules": {
				JsonPathRules: []*JsonPathRule{
					{Name: "EMAIL", Path: &Regexp{Regexp: regexp.MustCompile("auth")}},
				},
			},
			"multi-json": {
				JsonPathRules: []*JsonPathRule{
					{Name: "EMAIL1", Path: &Regexp{Regexp: regexp.MustCompile("auth")}},
					{Name: "EMAIL2", Path: &Regexp{Regexp: regexp.MustCompile("auth")}},
					{Name: "EMAIL3", Path: &Regexp{Regexp: regexp.MustCompile("auth")}},
					{Name: "EMAIL4", Path: &Regexp{Regexp: regexp.MustCompile("auth")}},
					{Name: "EMAIL5", Path: &Regexp{Regexp: regexp.MustCompile("auth")}},
				},
			},
		},
	}

	shortLine := `{"timestamp":"2024-07-03T12:34:56Z","level":"info","message":"Inbound traffic","auth":"Bearer example.com","source":{"ip":"192.168.1.1","service":"user-service"},"destination":{"ip":"192.168.1.2","service":"orders-service"}}`
	longerLine := `{"timestamp":"2024-07-03T12:34:56Z","level":"info","message":"Inbound traffic","auth":"Bearer example.com","source":{"ip":"192.168.1.1","service":"user-service","namespace":"default","labels":{"app":"user","version":"v1"},"ports":{"http":80,"https":443}},"destination":{"ip":"192.168.1.2","service":"orders-service","namespace":"default","labels":{"app":"orders","version":"v2"},"ports":{"http":8080,"https":8443}},"request":{"method":"GET","path":"/api/orders","headers":{"User-Agent":"curl/7.64.1","Accept":"*/*"},"body":""},"response":{"status_code":200,"headers":{"Content-Type":"application/json","Content-Length":"123"},"body":"{\"order_id\":\"12345\",\"status\":\"shipped\"}"},"duration":"123ms","trace_id":"abcdef1234567890","span_id":"1234567890abcdef"}`

	tests := []struct {
		name    string
		appName string
		doc     string
	}{
		{name: "short: 1 static rule", appName: "default", doc: shortLine},
		{name: "short: 5 static rules", appName: "multi-static", doc: shortLine},
		{name: "short: 1 json rule", appName: "json-rules", doc: shortLine},
		{name: "short: 5 json rules", appName: "multi-json", doc: shortLine},
		{name: "long: 1 static rule", appName: "default", doc: longerLine},
		{name: "long: 5 static rules", appName: "multi-static", doc: longerLine},
		{name: "long: 1 json rule", appName: "json-rules", doc: longerLine},
		{name: "long: 5 json rules", appName: "multi-json", doc: longerLine},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				redactor.Redact(tt.appName, tt.doc)
			}
		})
	}
}
