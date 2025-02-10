package stringlikeobject

import (
	"sort"
	"strconv"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
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
	return lo.EveryBy([]rune(str), func(c rune) bool {
		return c >= '0' && c <= '9'
	})
}

func ToString(obj map[string]any) string {
	keys := lo.Map(lo.Keys(obj), func(key string, _ int) int {
		numKey, _ := strconv.Atoi(key)
		return numKey
	})
	sort.Ints(keys)

	values := lo.Map(keys, func(key, _ int) string {
		return utils.ToString(obj[strconv.Itoa(key)])
	})
	return strings.Join(values, "")
}
