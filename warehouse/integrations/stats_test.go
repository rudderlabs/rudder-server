package integrations

import (
	"github.com/rudderlabs/rudder-go-kit/stats"
	"testing"
)

func BenchmarkStat(b *testing.B) {
	ss := stats.Default

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ss.NewTaggedStat("test", "count", stats.Tags{"test": "test"})
	}
}
