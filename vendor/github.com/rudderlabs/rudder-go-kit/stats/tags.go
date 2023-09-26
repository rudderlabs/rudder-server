package stats

import (
	"sort"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// Tags is a map of key value pairs
type Tags map[string]string

// Strings returns all key value pairs as an ordered list of strings, sorted by increasing key order
func (t Tags) Strings() []string {
	if len(t) == 0 {
		return nil
	}
	res := make([]string, 0, len(t)*2)
	// sorted by tag name (!important for consistent map iteration order)
	tagNames := make([]string, 0, len(t))
	for n := range t {
		tagNames = append(tagNames, n)
	}
	sort.Strings(tagNames)
	for _, tagName := range tagNames {
		tagVal := t[tagName]
		res = append(res, strings.ReplaceAll(tagName, ":", "-"), strings.ReplaceAll(tagVal, ":", "-"))
	}
	return res
}

// String returns all key value pairs as a single string, separated by commas, sorted by increasing key order
func (t Tags) String() string {
	return strings.Join(t.Strings(), ",")
}

// otelAttributes returns all key value pairs as a list of OpenTelemetry attributes
func (t Tags) otelAttributes() []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(t))
	for k, v := range t {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attrs
}
