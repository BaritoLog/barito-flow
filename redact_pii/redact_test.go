package redact_pii

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedactJSONValueStream(t *testing.T) {
	tests := []struct {
		name  string
		want  string
		input string
	}{
		{
			name:  "Normal email string",
			input: `{"email":"test@gmail.com"}`,
			want:  `{"email":"[REDACTED]"}`,
		},
		{
			name:  "Email string with .",
			input: `{"email":"test.123-@gmail.com"}`,
			want:  `{"email":"[REDACTED]"}`,
		},
		{
			name:  "Format dd/mm/yyyy",
			input: `{"date of birth":"08/23/1999"}`,
			want:  `{"date of birth":"[REDACTED]"}`,
		},
		{
			name:  "Fromat dd-mm-yyy",
			input: `{"dob":"08-29-1978"}`,
			want:  `{"dob":"[REDACTED]"}`,
		},
		{
			name:  "Array of dates",
			input: `{"dates":["1990-01-01","2020-02-29","not a date"]}`,
			want:  `{"dates":["[REDACTED]","[REDACTED]","not a date"]}`,
		},
		{
			name:  "Nested date",
			input: `{"user":{"dob":"1990-01-01"}}`,
			want:  `{"user":{"dob":"[REDACTED]"}}`,
		},
		{
			name:  "Partial date strings",
			input: `{"dob":"2022-12"}`,
			want:  `{"dob":"2022-12"}`,
		},
		{
			name:  "Non-date string",
			input: `{"dob":"not a date"}`,
			want:  `{"dob":"not a date"}`,
		},
		{
			name:  "Normal Gender match",
			input: `{"gender":"male"}`,
			want:  `{"gender":"[REDACTED]"}`,
		},
		{
			name:  "Substring Gender match",
			input: `{"gender":"Identified as male"}`,
			want:  `{"gender":"[REDACTED]"}`,
		},
		{
			name:  "Normal phone no. match",
			input: `{"phone no":"123-456-7890"}`,
			want:  `{"phone no":"[REDACTED]"}`,
		},
		{
			name:  "Substring phone no. match",
			input: `{"text":"phone: +911234567890"}`,
			want:  `{"text":"phone: +[REDACTED]"}`,
		},
		{
			name:  "Combined json string",
			input: `{"address":"123 Main St", "dob":"01-12-1990", "email":"test@example.com", "gender":"Male", "phone":"123-456-7890"}`,
			want:  `{"address":"123 [REDACTED]","dob":"[REDACTED]","email":"[REDACTED]","gender":"[REDACTED]","phone":"[REDACTED]"}`,
		},
		
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RedactJSONValues(tc.input)
			fmt.Println("got: ", got)
			fmt.Println("want: ", tc.want)
			if err != nil {
				t.Fatalf("expected: %v, got: %v", tc.want, got)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}
