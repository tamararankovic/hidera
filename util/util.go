package util

import "strings"

func SplitAndTrim(s string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func EnsureSameLength(slice *[]string, n int) {
	if len(*slice) < n {
		padding := make([]string, n-len(*slice))
		*slice = append(*slice, padding...)
	}
}
