package warehouseutils

import (
	"testing"
)

/*
The expected benchmark shows the best possible scenario vs the worst possible one (unexpected).

BenchmarkGetQueryType/expected-20         	 2443971		498.8 ns/op
BenchmarkGetQueryType/unexpected-20       	  308714		4088 ns/op (<= 0.004ms)
*/
func BenchmarkGetQueryType(b *testing.B) {
	b.Run("expected", func(b *testing.B) {
		query := "SELECT * FROM t1"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = GetQueryType(query)
		}
	})
	b.Run("unexpected", func(b *testing.B) {
		query := "\t\n\n  \t\n\n  some unexpected query"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = GetQueryType(query)
		}
	})
}
