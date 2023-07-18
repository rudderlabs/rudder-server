package warehouseutils

import (
	"testing"
)

/*
BenchmarkGetQueryType/expected-24			4825515			248.1 ns/op
BenchmarkGetQueryType/unknown-24			922964			1296 ns/op
BenchmarkGetQueryType/empty-24				824426			1253 ns/op
*/
func BenchmarkGetQueryType(b *testing.B) {
	b.Run("expected", func(b *testing.B) {
		query := "\t\n\n  \t\n\n  seLeCt * from table"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = GetQueryType(query)
		}
	})
	b.Run("unexpected", func(b *testing.B) {
		query := "\t\n\n  \t\n\n  something * from table"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = GetQueryType(query)
		}
	})
	b.Run("empty", func(b *testing.B) {
		query := "\t\n\n  \t\n\n  "

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = GetQueryType(query)
		}
	})
}
