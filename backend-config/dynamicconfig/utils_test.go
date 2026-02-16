package dynamicconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/backend-config/dynamicconfig"
)

// Unit tests for ContainsPattern function
func TestContainsPattern(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected bool
	}{
		{
			name:     "nil map",
			input:    nil,
			expected: false,
		},
		{
			name: "simple pattern",
			input: map[string]any{
				"key": "{{ path.to.value || \"default\" }}",
			},
			expected: true,
		},
		{
			name: "nested map with pattern",
			input: map[string]any{
				"outer": map[string]any{
					"inner": "{{ path.to.value || \"default\" }}",
				},
			},
			expected: true,
		},
		{
			name: "array of objects with pattern",
			input: map[string]any{
				"items": []any{
					map[string]any{
						"name": "{{ item.name || \"unnamed\" }}",
					},
				},
			},
			expected: true,
		},
		{
			name: "array with string element containing pattern",
			input: map[string]any{
				"items": []any{
					"normal string",
					"{{ item.value || \"default\" }}",
				},
			},
			expected: true,
		},
		{
			name: "pattern with number as default value",
			input: map[string]any{
				"timeout": "{{ message.traits.key || 1233 }}",
			},
			expected: true,
		},
		{
			name: "no patterns",
			input: map[string]any{
				"key": "normal string",
				"nested": map[string]any{
					"inner": 123,
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dynamicconfig.ContainsPattern(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Benchmark checking if a map contains a pattern (optimized version)
func BenchmarkContainsPattern_SinglePattern(b *testing.B) {
	data := map[string]any{
		"key": "{{ path.to.value || \"default\" }}",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dynamicconfig.ContainsPattern(data)
	}
}

// Benchmark checking if a complex nested map contains a pattern
func BenchmarkContainsPattern_ComplexMap(b *testing.B) {
	data := map[string]any{
		"key1": "{{ path1 || \"default1\" }}",
		"key2": "normal string",
		"nested": map[string]any{
			"inner1": "{{ path2 || \"default2\" }}",
			"inner2": 123,
			"deepNested": map[string]any{
				"deep": "{{ path3 || \"default3\" }}",
			},
		},
		"array": []any{
			map[string]any{
				"item1": "{{ path4 || \"default4\" }}",
			},
			"{{ path5 || \"default5\" }}",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dynamicconfig.ContainsPattern(data)
	}
}

// Benchmark checking if a map with no patterns contains a pattern
func BenchmarkContainsPattern_NoPatterns(b *testing.B) {
	data := map[string]any{
		"key1": "normal string 1",
		"key2": "normal string 2",
		"nested": map[string]any{
			"inner1": "normal string 3",
			"inner2": 123,
			"deepNested": map[string]any{
				"deep": "normal string 4",
			},
		},
		"array": []any{
			map[string]any{
				"item1": "normal string 5",
			},
			"normal string 6",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dynamicconfig.ContainsPattern(data)
	}
}
