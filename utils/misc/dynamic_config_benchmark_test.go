package misc_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Helper function to create a map with a pattern at a specific depth
func createNestedMapWithPattern(depth, patternAtDepth int) map[string]interface{} {
	if depth == 0 {
		if patternAtDepth == 0 {
			return map[string]interface{}{
				"key": "{{ path.to.value || \"default\" }}",
			}
		}
		return map[string]interface{}{
			"key": "normal string",
		}
	}

	var innerMap map[string]interface{}
	if depth == patternAtDepth {
		innerMap = map[string]interface{}{
			"key": "{{ path.to.value || \"default\" }}",
		}
	} else {
		innerMap = createNestedMapWithPattern(depth-1, patternAtDepth)
	}

	return map[string]interface{}{
		"level": innerMap,
	}
}

// Helper function to create a map with multiple patterns
func createMapWithMultiplePatterns(numPatterns int) map[string]interface{} {
	result := make(map[string]interface{})

	for i := 0; i < numPatterns; i++ {
		key := "key" + string(rune('0'+i))
		result[key] = "{{ path.to.value" + string(rune('0'+i)) + " || \"default\" }}"
	}

	return result
}

// Helper function to create a large map with a pattern at the end
func createLargeMapWithPatternAtEnd(size int) map[string]interface{} {
	result := make(map[string]interface{})

	for i := 0; i < size-1; i++ {
		key := "key" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+(i/100)%10))
		result[key] = "normal string " + string(rune('0'+i%10))
	}

	// Add pattern at the end
	result["lastKey"] = "{{ path.to.value || \"default\" }}"

	return result
}

// Benchmark finding all patterns in a map with a single pattern
func BenchmarkFindAllPatterns_SinglePattern(b *testing.B) {
	data := map[string]interface{}{
		"key": "{{ path.to.value || \"default\" }}",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		patterns := misc.FindDynamicConfigPatterns(data)
		_ = len(patterns)
	}
}

// Benchmark checking if a map contains a pattern (optimized version)
func BenchmarkContainsPattern_SinglePattern(b *testing.B) {
	data := map[string]interface{}{
		"key": "{{ path.to.value || \"default\" }}",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = misc.ContainsDynamicConfigPattern(data)
	}
}

// Benchmark finding all patterns in a deeply nested map
func BenchmarkFindAllPatterns_DeeplyNested(b *testing.B) {
	// Create a map with a pattern at depth 10
	data := createNestedMapWithPattern(10, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		patterns := misc.FindDynamicConfigPatterns(data)
		_ = len(patterns)
	}
}

// Benchmark checking if a deeply nested map contains a pattern
func BenchmarkContainsPattern_DeeplyNested(b *testing.B) {
	// Create a map with a pattern at depth 10
	data := createNestedMapWithPattern(10, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = misc.ContainsDynamicConfigPattern(data)
	}
}

// Benchmark finding all patterns in a map with multiple patterns
func BenchmarkFindAllPatterns_MultiplePatterns(b *testing.B) {
	// Create a map with 10 patterns
	data := createMapWithMultiplePatterns(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		patterns := misc.FindDynamicConfigPatterns(data)
		_ = len(patterns)
	}
}

// Benchmark checking if a map with multiple patterns contains a pattern
func BenchmarkContainsPattern_MultiplePatterns(b *testing.B) {
	// Create a map with 10 patterns
	data := createMapWithMultiplePatterns(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = misc.ContainsDynamicConfigPattern(data)
	}
}

// Benchmark finding all patterns in a large map with a pattern at the end
func BenchmarkFindAllPatterns_LargeMapPatternAtEnd(b *testing.B) {
	// Create a large map with 1000 entries and a pattern at the end
	data := createLargeMapWithPatternAtEnd(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		patterns := misc.FindDynamicConfigPatterns(data)
		_ = len(patterns)
	}
}

// Benchmark checking if a large map with a pattern at the end contains a pattern
func BenchmarkContainsPattern_LargeMapPatternAtEnd(b *testing.B) {
	// Create a large map with 1000 entries and a pattern at the end
	data := createLargeMapWithPatternAtEnd(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = misc.ContainsDynamicConfigPattern(data)
	}
}

// Benchmark finding all patterns in a map with no patterns
func BenchmarkFindAllPatterns_NoPatterns(b *testing.B) {
	data := map[string]interface{}{
		"key1": "normal string",
		"key2": 123,
		"key3": map[string]interface{}{
			"nested": "also normal",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		patterns := misc.FindDynamicConfigPatterns(data)
		_ = len(patterns)
	}
}

// Benchmark checking if a map with no patterns contains a pattern
func BenchmarkContainsPattern_NoPatterns(b *testing.B) {
	data := map[string]interface{}{
		"key1": "normal string",
		"key2": 123,
		"key3": map[string]interface{}{
			"nested": "also normal",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = misc.ContainsDynamicConfigPattern(data)
	}
}
