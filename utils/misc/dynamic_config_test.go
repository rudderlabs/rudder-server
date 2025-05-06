package misc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestFindDynamicConfigPatterns(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		expected []misc.DynamicConfigPattern
	}{
		{
			name:     "nil map",
			input:    nil,
			expected: []misc.DynamicConfigPattern{},
		},
		{
			name: "simple pattern",
			input: map[string]interface{}{
				"key": "{{ path.to.value || \"default\" }}",
			},
			expected: []misc.DynamicConfigPattern{
				{
					FullMatch: "{{ path.to.value || \"default\" }}",
					Path:      "path.to.value",
					Fallback:  "default",
					Location:  []string{"key"},
				},
			},
		},
		{
			name: "pattern with boolean fallback",
			input: map[string]interface{}{
				"enabled": "{{ features.enabled || true }}",
			},
			expected: []misc.DynamicConfigPattern{
				{
					FullMatch: "{{ features.enabled || true }}",
					Path:      "features.enabled",
					Fallback:  "true",
					Location:  []string{"enabled"},
				},
			},
		},
		{
			name: "pattern with numeric fallback",
			input: map[string]interface{}{
				"count": "{{ metrics.count || 42 }}",
			},
			expected: []misc.DynamicConfigPattern{
				{
					FullMatch: "{{ metrics.count || 42 }}",
					Path:      "metrics.count",
					Fallback:  "42",
					Location:  []string{"count"},
				},
			},
		},
		{
			name: "pattern with complex fallback",
			input: map[string]interface{}{
				"config": "{{ user.config || {\"key\": \"value\"} }}",
			},
			expected: []misc.DynamicConfigPattern{
				{
					FullMatch: "{{ user.config || {\"key\": \"value\"} }}",
					Path:      "user.config",
					Fallback:  "{\"key\": \"value\"}",
					Location:  []string{"config"},
				},
			},
		},
		{
			name: "nested map with pattern",
			input: map[string]interface{}{
				"outer": map[string]interface{}{
					"inner": "{{ nested.path || \"fallback\" }}",
				},
			},
			expected: []misc.DynamicConfigPattern{
				{
					FullMatch: "{{ nested.path || \"fallback\" }}",
					Path:      "nested.path",
					Fallback:  "fallback",
					Location:  []string{"outer", "inner"},
				},
			},
		},
		{
			name: "array with pattern",
			input: map[string]interface{}{
				"items": []interface{}{
					"normal string",
					"{{ array.item || \"default\" }}",
					map[string]interface{}{
						"nested": "{{ nested.array.item || \"nested default\" }}",
					},
				},
			},
			expected: []misc.DynamicConfigPattern{
				{
					FullMatch: "{{ array.item || \"default\" }}",
					Path:      "array.item",
					Fallback:  "default",
					Location:  []string{"items", "[1]"},
				},
				{
					FullMatch: "{{ nested.array.item || \"nested default\" }}",
					Path:      "nested.array.item",
					Fallback:  "nested default",
					Location:  []string{"items", "[2]", "nested"},
				},
			},
		},
		{
			name: "multiple patterns in one string",
			input: map[string]interface{}{
				"complex": "Start {{ first.path || \"first\" }} middle {{ second.path || \"second\" }} end",
			},
			expected: []misc.DynamicConfigPattern{
				{
					FullMatch: "{{ first.path || \"first\" }}",
					Path:      "first.path",
					Fallback:  "first",
					Location:  []string{"complex"},
				},
				{
					FullMatch: "{{ second.path || \"second\" }}",
					Path:      "second.path",
					Fallback:  "second",
					Location:  []string{"complex"},
				},
			},
		},
		{
			name: "no patterns",
			input: map[string]interface{}{
				"key1": "normal string",
				"key2": 123,
				"key3": map[string]interface{}{
					"nested": "also normal",
				},
			},
			expected: []misc.DynamicConfigPattern{},
		},
		{
			name: "pattern with whitespace variations",
			input: map[string]interface{}{
				"whitespace": "{{path.to.value||\"default\"}}",
				"more_space": "{{ path.to.value  ||  \"default with spaces\"  }}",
			},
			expected: []misc.DynamicConfigPattern{
				{
					FullMatch: "{{path.to.value||\"default\"}}",
					Path:      "path.to.value",
					Fallback:  "default",
					Location:  []string{"whitespace"},
				},
				{
					FullMatch: "{{ path.to.value  ||  \"default with spaces\"  }}",
					Path:      "path.to.value",
					Fallback:  "default with spaces",
					Location:  []string{"more_space"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := misc.FindDynamicConfigPatterns(tt.input)

			// Check length first
			assert.Equal(t, len(tt.expected), len(result), "Number of patterns found should match expected")

			// For each expected pattern, verify it exists in the result
			for _, expected := range tt.expected {
				found := false
				for _, actual := range result {
					if expected.FullMatch == actual.FullMatch &&
						expected.Path == actual.Path &&
						expected.Fallback == actual.Fallback {
						// Check location path
						assert.Equal(t, expected.Location, actual.Location, "Location path should match")
						found = true
						break
					}
				}
				assert.True(t, found, "Expected pattern not found: %v", expected)
			}
		})
	}
}

func TestContainsDynamicConfigPattern(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		expected bool
	}{
		{
			name: "contains pattern",
			input: map[string]interface{}{
				"key": "{{ path.to.value || \"default\" }}",
			},
			expected: true,
		},
		{
			name: "contains pattern with boolean fallback",
			input: map[string]interface{}{
				"enabled": "{{ features.enabled || true }}",
			},
			expected: true,
		},
		{
			name: "contains pattern with numeric fallback",
			input: map[string]interface{}{
				"count": "{{ metrics.count || 42 }}",
			},
			expected: true,
		},
		{
			name: "contains pattern with complex fallback",
			input: map[string]interface{}{
				"config": "{{ user.config || {\"key\": \"value\"} }}",
			},
			expected: true,
		},
		{
			name: "nested pattern",
			input: map[string]interface{}{
				"outer": map[string]interface{}{
					"inner": "{{ nested.path || \"fallback\" }}",
				},
			},
			expected: true,
		},
		{
			name: "no patterns",
			input: map[string]interface{}{
				"key1": "normal string",
				"key2": 123,
			},
			expected: false,
		},
		{
			name:     "empty map",
			input:    map[string]interface{}{},
			expected: false,
		},
		{
			name: "pattern inside array",
			input: map[string]interface{}{
				"items": []interface{}{
					"normal string",
					"{{ array.item || \"default\" }}",
					map[string]interface{}{
						"nested": "normal string",
					},
				},
			},
			expected: true,
		},
		{
			name:     "nil map",
			input:    nil,
			expected: false,
		},
		{
			name: "pattern in nested object inside array",
			input: map[string]interface{}{
				"items": []interface{}{
					"normal string",
					map[string]interface{}{
						"config": "{{ nested.object.in.array || \"default\" }}",
					},
				},
			},
			expected: true,
		},
		{
			name: "pattern in deeply nested object inside array",
			input: map[string]interface{}{
				"items": []interface{}{
					"normal string",
					map[string]interface{}{
						"level1": map[string]interface{}{
							"level2": map[string]interface{}{
								"level3": "{{ deeply.nested.object.in.array || \"default\" }}",
							},
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := misc.ContainsDynamicConfigPattern(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
