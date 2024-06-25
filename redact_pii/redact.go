package redact_pii

import (
	"encoding/json"
	"fmt"
	"regexp"
)

var (
	EmailRegex       = regexp.MustCompile(`(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}`)
	PhoneRegex       = regexp.MustCompile(`\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b`)
	AddressRegex     = regexp.MustCompile(`(?i)(address|st|street|road|rd|avenue|ave|blvd|boulevard|lane|ln|dr|drive)\s+\d{1,5}\s+\w+(\s+\w+)*`)
	NameRegex        = regexp.MustCompile(`(?i)\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b`)
	DobRegex         = regexp.MustCompile(`\b((\d{4})[-/.](\d{2})[-/.](\d{2}))|((\d{2})[-/.](\d{2})[-/.](\d{4}))\b`)
	DocumentRegex    = regexp.MustCompile(`\b[0-9]{4}[ ]?[0-9]{4}[ ]?[0-9]{4}\b`)
	BankAccountRegex = regexp.MustCompile(`\b\d{9,18}\b`)
	CreditScoreRegex = regexp.MustCompile(`(?i)\b(?:credit score|score|credit):?\s*\d{3}\b`)
	SexRegex         = regexp.MustCompile(`(?i)\b(?:Male|Female|Other)\b`)
)

func RedactPIIData(jsonStr string) string {
	redacted := EmailRegex.ReplaceAllString(jsonStr, "[REDACTED]")
	redacted = PhoneRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = AddressRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = NameRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = DobRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = DocumentRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = BankAccountRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = CreditScoreRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = SexRegex.ReplaceAllString(redacted, "[REDACTED]")
	return redacted
}

func RedactJSONValues(jsonStr string) (string, error) {
	var jsonData map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &jsonData); err != nil {
		return "", err
	}

	redactMap(jsonData)

	redactedJSON, err := json.Marshal(jsonData)
	if err != nil {
		return "", err
	}

	return string(redactedJSON), nil
}

func redactMap(data map[string]interface{}) {
	for key, value := range data {
		switch v := value.(type) {
		case string:
			data[key] = RedactPIIData(v)
		case float64:
			data[key] = RedactPIIData(fmt.Sprintf("%v", v))
		case map[string]interface{}:
			redactMap(v)
		case []interface{}:
			redactSlice(v)
		}
	}
}

func redactSlice(data []interface{}) {
	for i, value := range data {
		switch v := value.(type) {
		case string:
			data[i] = RedactPIIData(v)
		case float64:
			data[i] = RedactPIIData(fmt.Sprintf("%v", v))
		case map[string]interface{}:
			redactMap(v)
		case []interface{}:
			redactSlice(v)
		}
	}
}
