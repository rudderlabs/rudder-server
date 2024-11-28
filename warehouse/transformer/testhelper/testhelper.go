package testhelper

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

func AddRandomColumns(eventPayload string, numColumns int) string {
	return fmt.Sprintf(eventPayload, strings.Join(
		lo.RepeatBy(numColumns, func(index int) string {
			return fmt.Sprintf(`"random_column_%d": "random_value_%d"`, index, index)
		}), ",",
	))
}

func AddLargeColumns(eventPayload string, numColumns int) string {
	return fmt.Sprintf(eventPayload, strings.Join(
		lo.RepeatBy(numColumns, func(index int) string {
			return fmt.Sprintf(`"large_column_`+strings.Repeat("a", 1000)+`": "large_value_%d"`, index)
		}), ",",
	))
}

func AddNestedLevels(eventPayload string, numLevels int) string {
	var nestedBuilder strings.Builder

	for i := numLevels; i > 0; i-- {
		if i < numLevels {
			nestedBuilder.WriteString(", ")
		}
		nestedBuilder.WriteString(fmt.Sprintf(`"nested_level_%d": {`, i))
	}
	for i := 0; i < numLevels; i++ {
		nestedBuilder.WriteString("}")
		if i < numLevels-1 {
			nestedBuilder.WriteString(", ")
		}
	}
	return strings.Replace(eventPayload, "{}", "{"+nestedBuilder.String()+"}", 1)
}
