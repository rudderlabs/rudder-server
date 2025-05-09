package dynamicconfig

import (
	"regexp"
)

// Regular expression to match the pattern {{ path || fallback }}
// The pattern allows for any fallback value (string, boolean, number, etc.)
var dynamicConfigRegex = regexp.MustCompile(`\{\{(.*?)\|\|(.*?)\}\}`)

// ContainsPattern checks if a map or any of its nested maps contains
// at least one template pattern in the format {{ some_json_path || "someFallbackValue" }}.
// It returns true if at least one pattern is found, false otherwise.
// This function returns immediately upon finding the first pattern for better performance.
func ContainsPattern(data map[string]interface{}) bool {
	if data == nil {
		return false
	}
	return containsPatternRecursive(data)
}

// containsPatternRecursive is a helper function that recursively traverses the map
// and returns true as soon as it finds a template pattern.
func containsPatternRecursive(data map[string]interface{}) bool {
	for _, value := range data {
		switch v := value.(type) {
		case string:
			// Check if the string contains a template pattern
			if dynamicConfigRegex.MatchString(v) {
				return true
			}
		case map[string]interface{}:
			// Recursively check nested maps
			if containsPatternRecursive(v) {
				return true
			}
		case []interface{}:
			// Check each element in the array
			for _, item := range v {
				if nestedMap, ok := item.(map[string]interface{}); ok {
					// If the array element is a map, recursively check it
					if containsPatternRecursive(nestedMap) {
						return true
					}
				} else if strValue, ok := item.(string); ok {
					// If the array element is a string, check for patterns
					if dynamicConfigRegex.MatchString(strValue) {
						return true
					}
				}
			}
		}
	}
	return false
}
