package misc

import (
	"regexp"
	"strings"
)

// Regular expression to match the pattern {{ path || fallback }}
// The pattern allows for any fallback value (string, boolean, number, etc.)
var dynamicConfigRegex = regexp.MustCompile(`\{\{(.*?)\|\|(.*?)\}\}`)

// DynamicConfigPattern represents a found template pattern in the format {{ path || fallback }}
type DynamicConfigPattern struct {
	// FullMatch is the entire matched string including the {{ }} delimiters
	FullMatch string
	// Path is the JSON path part before the || operator
	Path string
	// Fallback is the fallback value after the || operator
	Fallback string
	// Location is the key path where this pattern was found in the map
	Location []string
}

// FindDynamicConfigPatterns recursively searches for template patterns in the format
// {{ some_json_path || "someFallbackValue" }} in a map and its nested maps.
// It returns a slice of DynamicConfigPattern objects containing information about each match.
// If data is nil, an empty slice is returned.
func FindDynamicConfigPatterns(data map[string]interface{}) []DynamicConfigPattern {
	var patterns []DynamicConfigPattern
	if data == nil {
		return patterns
	}
	findPatternsRecursive(data, []string{}, &patterns)
	return patterns
}

// findPatternsRecursive is a helper function that recursively traverses the map
// and collects all template patterns found in string values.
func findPatternsRecursive(data map[string]interface{}, path []string, patterns *[]DynamicConfigPattern) {
	for key, value := range data {
		currentPath := append(path, key)

		switch v := value.(type) {
		case string:
			// Check if the string contains a template pattern
			foundPatterns := extractDynamicConfigPatterns(v, currentPath)
			if len(foundPatterns) > 0 {
				*patterns = append(*patterns, foundPatterns...)
			}
		case map[string]interface{}:
			// Recursively check nested maps
			findPatternsRecursive(v, currentPath, patterns)
		case []interface{}:
			// Check each element in the array
			for i, item := range v {
				if nestedMap, ok := item.(map[string]interface{}); ok {
					// If the array element is a map, recursively check it
					arrayPath := append(currentPath, "["+string(rune('0'+i))+"]")
					findPatternsRecursive(nestedMap, arrayPath, patterns)
				} else if strValue, ok := item.(string); ok {
					// If the array element is a string, check for patterns
					arrayPath := append(currentPath, "["+string(rune('0'+i))+"]")
					foundPatterns := extractDynamicConfigPatterns(strValue, arrayPath)
					if len(foundPatterns) > 0 {
						*patterns = append(*patterns, foundPatterns...)
					}
				}
			}
		}
	}
}

// extractDynamicConfigPatterns extracts all template patterns from a string.
// It returns a slice of DynamicConfigPattern objects for each match found.
func extractDynamicConfigPatterns(s string, location []string) []DynamicConfigPattern {
	var patterns []DynamicConfigPattern

	// Use the pre-compiled regex to find all matches
	matches := dynamicConfigRegex.FindAllStringSubmatch(s, -1)

	for _, match := range matches {
		if len(match) >= 3 {
			fullMatch := match[0]
			pathValue := strings.TrimSpace(match[1])
			fallback := strings.TrimSpace(match[2])

			// Remove quotes from fallback if it's a quoted string
			if strings.HasPrefix(fallback, "\"") && strings.HasSuffix(fallback, "\"") {
				fallback = fallback[1 : len(fallback)-1]
			}

			pattern := DynamicConfigPattern{
				FullMatch: fullMatch,
				Path:      pathValue,
				Fallback:  fallback,
				Location:  append([]string{}, location...),
			}
			patterns = append(patterns, pattern)
		}
	}

	return patterns
}

// ContainsDynamicConfigPattern checks if a map or any of its nested maps contains
// at least one template pattern in the format {{ some_json_path || "someFallbackValue" }}.
// It returns true if at least one pattern is found, false otherwise.
// This function returns immediately upon finding the first pattern for better performance.
func ContainsDynamicConfigPattern(data map[string]interface{}) bool {
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
