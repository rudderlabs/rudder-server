package partitionbuffer

import "testing"

func BenchmarkReadOnlyMap(b *testing.B) {
	// Prepare test data
	smallMap := make(map[int]string)
	for i := range 10 {
		smallMap[i] = "value"
	}

	mediumMap := make(map[int]string)
	for i := range 1000 {
		mediumMap[i] = "value"
	}

	largeMap := make(map[int]string)
	for i := range 100000 {
		largeMap[i] = "value"
	}

	b.Run("Get/Small/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(smallMap)
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			_, _ = rom.Get(i % 10)
		}
	})

	b.Run("Get/Small/StandardMap", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, _ = smallMap[i%10] // nolint: staticcheck
		}
	})

	b.Run("Get/Medium/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(mediumMap)
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			_, _ = rom.Get(i % 1000)
		}
	})

	b.Run("Get/Medium/StandardMap", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, _ = mediumMap[i%1000] // nolint: staticcheck
		}
	})

	b.Run("Get/Large/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(largeMap)
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			_, _ = rom.Get(i % 100000)
		}
	})

	b.Run("Get/Large/StandardMap", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, _ = largeMap[i%100000] // nolint: staticcheck
		}
	})

	b.Run("Has/Medium/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(mediumMap)
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			_ = rom.Has(i % 1000)
		}
	})

	b.Run("Has/Medium/StandardMap", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, _ = mediumMap[i%1000] // nolint: staticcheck
		}
	})

	b.Run("Len/Medium/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(mediumMap)
		b.ResetTimer()
		for b.Loop() {
			_ = rom.Len()
		}
	})

	b.Run("Len/Medium/StandardMap", func(b *testing.B) {
		for b.Loop() {
			_ = len(mediumMap)
		}
	})

	b.Run("ForEach/Medium/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(mediumMap)
		b.ResetTimer()
		for b.Loop() {
			count := 0
			rom.ForEach(func(k int, v string) {
				count++
			})
		}
	})

	b.Run("ForEach/Medium/StandardMap", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			count := 0
			for range mediumMap {
				count++
			}
		}
	})

	b.Run("Append/Small/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(smallMap)
		appendData := map[int]string{100: "new"}
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			_ = rom.Append(appendData)
		}
	})

	b.Run("Keys/Medium/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(mediumMap)
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			count := 0
			for range rom.Keys() {
				count++
			}
		}
	})

	b.Run("Values/Medium/ReadOnlyMap", func(b *testing.B) {
		rom := newReadOnlyMap(mediumMap)
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			count := 0
			for range rom.Values() {
				count++
			}
		}
	})
}
