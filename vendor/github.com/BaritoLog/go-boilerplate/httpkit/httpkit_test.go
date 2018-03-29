package httpkit

import "testing"

func TestPathParameter(t *testing.T) {
	testcases := []struct {
		path     string
		key      string
		expected string
	}{
		{"/users/1", "users", "1"},
		{"/users/1/item/12", "users", "1"},
		{"/users/1/item/12", "shop", ""},
	}

	for _, tt := range testcases {
		get := PathParameter(tt.path, tt.key)
		if get != tt.expected {
			t.Fatalf("get '%s' instead of '%s'", get, tt.expected)
		}
	}
}
