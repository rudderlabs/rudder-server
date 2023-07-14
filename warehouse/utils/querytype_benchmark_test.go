package warehouseutils

import (
	"testing"
)

/*
BenchmarkGetQueryType/expected-24			4665873			255.6 ns/op
BenchmarkGetQueryType/unknown-24			921415			1299 ns/op
BenchmarkGetQueryType/empty-24				932668			1262 ns/op
*/
func BenchmarkGetQueryType(b *testing.B) {
	b.Run("expected", func(b *testing.B) {
		query := "\t\n\n  \t\n\n  seLeCt * from table"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = GetQueryType(query)
		}
	})
	b.Run("unexpected", func(b *testing.B) {
		query := "\t\n\n  \t\n\n  something * from table"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = GetQueryType(query)
		}
	})
	b.Run("empty", func(b *testing.B) {
		query := "\t\n\n  \t\n\n  "

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = GetQueryType(query)
		}
	})
}
