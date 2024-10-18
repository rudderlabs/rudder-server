package utils

import (
	"sort"
	"strconv"
)

func IsStringLikeObject(obj map[string]any) bool {
	if len(obj) == 0 {
		return false
	}

	minKey, maxKey := int(^uint(0)>>1), -1 // Initialize minKey as max int, maxKey as -1

	for key, value := range obj {
		if !isNonNegativeInteger(key) {
			return false
		}

		strValue, ok := value.(string)
		if !ok || len(strValue) != 1 {
			return false
		}

		numKey, err := strconv.Atoi(key)
		if err != nil {
			return false
		}

		minKey = min(minKey, numKey)
		maxKey = max(maxKey, numKey)
	}

	for i := minKey; i <= maxKey; i++ {
		if _, exists := obj[strconv.Itoa(i)]; !exists {
			return false
		}
	}
	return (minKey == 0 || minKey == 1) && maxKey-minKey+1 == len(obj)
}

func isNonNegativeInteger(str string) bool {
	if len(str) == 0 {
		return false
	}
	for _, c := range str {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func StringLikeObjectToString(obj map[string]any) any {
	keys := make([]int, 0, len(obj))
	for key := range obj {
		numKey, _ := strconv.Atoi(key)
		keys = append(keys, numKey)
	}

	sort.Ints(keys)

	result := ""
	for _, key := range keys {
		result += ToString(obj[strconv.Itoa(key)])
	}
	return result
}
