package httpkit

import "strings"

// PathParameter
func PathParameter(path, key string) string {
	i := strings.Index(path, key+"/")
	if i < 0 {
		return ""
	}
	s := path[i+len(key)+1:]
	j := strings.Index(s, "/")
	if j < 0 {
		return s
	} else {
		return s[:j]
	}
}
