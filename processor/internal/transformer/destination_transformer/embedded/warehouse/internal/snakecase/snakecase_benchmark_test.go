package snakecase

import (
	"testing"
)

// Representative column names as seen in warehouse transformations after the
// event is flattened. These are dominated by the common ASCII case (camelCase
// identifiers, snake_case keys, dotted context paths) with a few unicode and
// number-bearing inputs mixed in.
var benchInputs = []string{
	"messageId",
	"anonymousId",
	"userId",
	"sentAt",
	"timestamp",
	"receivedAt",
	"originalTimestamp",
	"channel",
	"request_ip",
	"review_id",
	"product_id",
	"rating",
	"review_body",
	"context_traits_name",
	"context_traits_email",
	"context_traits_logins",
	"context_ip",
	"userProperties_rating",
	"userProperties_review_body",
	"isISO8601",
	"xhr2Request",
	"walk500Miles",
	"enable24HFormat",
	"XMLHttpRequest",
	"alreadySnakeCasedColumnName",
	"someVeryLongColumnNameThatExceedsTheTypicalLengthOfAColumnNameUsedInWarehouse",
}

// BenchmarkToSnakeCaseWithNumbers measures the per-call cost of the snake_case
// conversion across a mixed, realistic set of column names. This is the raw
// hotspot observed in CPU profiles, exercised on every cache miss.
func BenchmarkToSnakeCaseWithNumbers(b *testing.B) {
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = ToSnakeCaseWithNumbers(benchInputs[i%len(benchInputs)])
		i++
	}
}

// BenchmarkToSnakeCase measures the underscore-divide-numbers variant.
func BenchmarkToSnakeCase(b *testing.B) {
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = ToSnakeCase(benchInputs[i%len(benchInputs)])
		i++
	}
}

// BenchmarkToSnakeCaseWithNumbers_Single isolates the cost per distinct input
// shape so we can see which categories (ASCII vs unicode-word path) dominate.
func BenchmarkToSnakeCaseWithNumbers_Single(b *testing.B) {
	cases := map[string]string{
		"plain_ascii":   "messageId",
		"snake_already": "context_traits_name",
		"with_numbers":  "walk500Miles",
		"all_upper":     "XMLHttpRequest",
		"long":          "someVeryLongColumnNameThatExceedsTheTypicalLengthOfAColumnNameUsedInWarehouse",
	}
	for name, in := range cases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = ToSnakeCaseWithNumbers(in)
			}
		})
	}
}
