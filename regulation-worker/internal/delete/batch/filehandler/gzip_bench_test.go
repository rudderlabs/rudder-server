package filehandler

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

// measurePeakHeap runs fn once and samples runtime.MemStats.HeapAlloc in a
// tight loop to approximate the peak live heap held during the call. Sampling
// adds some overhead so this is run outside the timed benchmark loop.
func measurePeakHeap(fn func()) uint64 {
	runtime.GC()

	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	var peak uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var ms runtime.MemStats
		t := time.NewTicker(50 * time.Microsecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > atomic.LoadUint64(&peak) {
					atomic.StoreUint64(&peak, ms.HeapAlloc)
				}
			}
		}
	}()

	fn()

	close(stop)
	<-done

	p := atomic.LoadUint64(&peak)
	if p < baseline.HeapAlloc {
		return 0
	}
	return p - baseline.HeapAlloc
}

func buildBenchRecords(numRecords, suppressEvery int, suppressedIDs []string) ([]byte, []model.User) {
	var buf bytes.Buffer
	for i := 0; i < numRecords; i++ {
		var id string
		if suppressEvery > 0 && i%suppressEvery == 0 {
			id = suppressedIDs[i%len(suppressedIDs)]
		} else {
			id = fmt.Sprintf("keep-user-%d", i)
		}
		fmt.Fprintf(&buf,
			`{"user_id": %q, "event": "track", "properties": {"page": "home", "idx": %d}, "context": {"app": {"name": "rudder-bench"}}}`+"\n",
			id, i)
	}
	users := make([]model.User, len(suppressedIDs))
	for i, id := range suppressedIDs {
		users[i] = model.User{ID: id}
	}
	return buf.Bytes(), users
}

var benchScenarios = []struct {
	name          string
	numRecords    int
	numSuppressed int
	suppressEvery int
}{
	{"records=1000/users=1/hit=10%", 1000, 1, 10},
	{"records=1000/users=10/hit=10%", 1000, 10, 10},
	{"records=10000/users=1/hit=10%", 10000, 1, 10},
	{"records=10000/users=10/hit=10%", 10000, 10, 10},
	{"records=10000/users=100/hit=10%", 10000, 100, 10},
	{"records=100000/users=10/hit=10%", 100000, 10, 10},
}

// buildLargePayloadRecords generates NDJSON lines of ~1 KB each by padding
// properties with extra data, simulating realistic large event payloads.
func buildLargePayloadRecords(numRecords, suppressEvery int, suppressedIDs []string) ([]byte, []model.User) {
	padding := strings.Repeat("x", 800)
	var buf bytes.Buffer
	buf.Grow(numRecords * 1024)
	for i := 0; i < numRecords; i++ {
		var id string
		if suppressEvery > 0 && i%suppressEvery == 0 {
			id = suppressedIDs[i%len(suppressedIDs)]
		} else {
			id = fmt.Sprintf("keep-user-%d", i)
		}
		fmt.Fprintf(&buf,
			`{"user_id": %q, "event": "track", "properties": {"page": "home", "idx": %d, "padding": "%s"}, "context": {"app": {"name": "rudder-bench"}}}`+"\n",
			id, i, padding)
	}
	users := make([]model.User, len(suppressedIDs))
	for i, id := range suppressedIDs {
		users[i] = model.User{ID: id}
	}
	return buf.Bytes(), users
}

func makeSuppressedIDs(n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("suppress-user-%d", i)
	}
	return ids
}

// BenchmarkRemoveIdentity
// BenchmarkRemoveIdentity/records=1000/users=1/hit=10%
// BenchmarkRemoveIdentity/records=1000/users=1/hit=10%-12         	     202	   5865799 ns/op	  23.35 MB/s	         0.3963 peak-heap-MB	  415204 B/op	     104 allocs/op
// BenchmarkRemoveIdentity/records=1000/users=10/hit=10%
// BenchmarkRemoveIdentity/records=1000/users=10/hit=10%-12        	      68	  17426360 ns/op	   7.86 MB/s	         0.3979 peak-heap-MB	  420861 B/op	     125 allocs/op
// BenchmarkRemoveIdentity/records=10000/users=1/hit=10%
// BenchmarkRemoveIdentity/records=10000/users=1/hit=10%-12        	      57	  19752813 ns/op	  70.31 MB/s	         4.832 peak-heap-MB	 5698321 B/op	     113 allocs/op
// BenchmarkRemoveIdentity/records=10000/users=10/hit=10%
// BenchmarkRemoveIdentity/records=10000/users=10/hit=10%-12       	       7	 145416923 ns/op	   9.55 MB/s	         4.833 peak-heap-MB	 6402260 B/op	     150 allocs/op
// BenchmarkRemoveIdentity/records=10000/users=100/hit=10%
// BenchmarkRemoveIdentity/records=10000/users=100/hit=10%-12      	       1	2605464500 ns/op	   0.53 MB/s	         4.840 peak-heap-MB	11245448 B/op	     631 allocs/op
// BenchmarkRemoveIdentity/records=100000/users=10/hit=10%
// BenchmarkRemoveIdentity/records=100000/users=10/hit=10%-12      	       1	2517548667 ns/op	   5.59 MB/s	        43.43 peak-heap-MB	95304536 B/op
func BenchmarkRemoveIdentity(b *testing.B) {
	ctx := context.Background()
	for _, sc := range benchScenarios {
		ids := makeSuppressedIDs(sc.numSuppressed)
		records, users := buildBenchRecords(sc.numRecords, sc.suppressEvery, ids)
		b.Run(sc.name, func(b *testing.B) {
			b.SetBytes(int64(len(records)))
			b.ReportAllocs()

			peak := measurePeakHeap(func() {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentity(ctx, users); err != nil {
					b.Fatal(err)
				}
			})
			b.ReportMetric(float64(peak)/(1024*1024), "peak-heap-MB")

			for i := 0; i < b.N; i++ {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentity(ctx, users); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRemoveIdentityRE
// BenchmarkRemoveIdentityRE/records=1000/users=1/hit=10%
// BenchmarkRemoveIdentityRE/records=1000/users=1/hit=10%-12         	    2970	    394019 ns/op	 347.68 MB/s	         0.2423 peak-heap-MB	  176908 B/op	    1036 allocs/op
// BenchmarkRemoveIdentityRE/records=1000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=1000/users=10/hit=10%-12        	    2955	    393509 ns/op	 348.13 MB/s	         0.2438 peak-heap-MB	  177243 B/op	    1039 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=1/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=1/hit=10%-12        	     292	   3992584 ns/op	 347.87 MB/s	         1.749 peak-heap-MB	 1733646 B/op	   10074 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=10/hit=10%-12       	     290	   4008821 ns/op	 346.46 MB/s	         1.747 peak-heap-MB	 1734717 B/op	   10077 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=100/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=100/hit=10%-12      	     284	   4113713 ns/op	 337.84 MB/s	         1.751 peak-heap-MB	 1738253 B/op	   10078 allocs/op
// BenchmarkRemoveIdentityRE/records=100000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=100000/users=10/hit=10%-12      	      26	  44146639 ns/op	 318.91 MB/s	        16.60 peak-heap-MB	17965420 B/op	  103892 allocs/op
func BenchmarkRemoveIdentityRE(b *testing.B) {
	ctx := context.Background()
	for _, sc := range benchScenarios {
		ids := makeSuppressedIDs(sc.numSuppressed)
		records, users := buildBenchRecords(sc.numRecords, sc.suppressEvery, ids)
		b.Run(sc.name, func(b *testing.B) {
			b.SetBytes(int64(len(records)))
			b.ReportAllocs()

			peak := measurePeakHeap(func() {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentityRE(ctx, users); err != nil {
					b.Fatal(err)
				}
			})
			b.ReportMetric(float64(peak)/(1024*1024), "peak-heap-MB")

			for i := 0; i < b.N; i++ {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentityRE(ctx, users); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRemoveIdentityPureGo
// BenchmarkRemoveIdentityPureGo/records=1000/users=1/hit=10%
// BenchmarkRemoveIdentityPureGo/records=1000/users=1/hit=10%-12         	   12310	     95045 ns/op	1441.33 MB/s	         0.1586 peak-heap-MB	  171278 B/op	    2001 allocs/op
// BenchmarkRemoveIdentityPureGo/records=1000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=1000/users=10/hit=10%-12        	   12943	     92809 ns/op	1476.06 MB/s	         0.1605 peak-heap-MB	  171733 B/op	    2004 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=1/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=1/hit=10%-12        	    1230	    923831 ns/op	1503.40 MB/s	         1.628 peak-heap-MB	 1714033 B/op	   20017 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=10/hit=10%-12       	    1267	    903366 ns/op	1537.46 MB/s	         1.631 peak-heap-MB	 1714449 B/op	   20019 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=100/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=100/hit=10%-12      	    1192	    977520 ns/op	1421.75 MB/s	         1.635 peak-heap-MB	 1717583 B/op	   20020 allocs/op
// BenchmarkRemoveIdentityPureGo/records=100000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=100000/users=10/hit=10%-12      	     118	   9021817 ns/op	1560.54 MB/s	        16.48 peak-heap-MB	17428971 B/op	  201699 allocs/op
func BenchmarkRemoveIdentityPureGo(b *testing.B) {
	ctx := context.Background()
	for _, sc := range benchScenarios {
		ids := makeSuppressedIDs(sc.numSuppressed)
		records, users := buildBenchRecords(sc.numRecords, sc.suppressEvery, ids)
		b.Run(sc.name, func(b *testing.B) {
			b.SetBytes(int64(len(records)))
			b.ReportAllocs()

			peak := measurePeakHeap(func() {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentityPureGo(ctx, users); err != nil {
					b.Fatal(err)
				}
			})
			b.ReportMetric(float64(peak)/(1024*1024), "peak-heap-MB")

			for i := 0; i < b.N; i++ {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentityPureGo(ctx, users); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Large-file benchmarks with ~1 KB payloads.
// Sed is excluded — it would take hours at these sizes.
// The 2 GB scenario requires ~6 GB of free RAM (input + output buffer + copy).
// Run with: go test -run=^$ -bench=BenchmarkLargeFile -benchtime=1x -timeout=30m
var largeFileScenarios = []struct {
	name          string
	numRecords    int
	numSuppressed int
	suppressEvery int
}{
	{"payload=1KB/file=100MB/users=10", 100_000, 10, 10},
	{"payload=1KB/file=500MB/users=10", 500_000, 10, 10},
	{"payload=1KB/file=2GB/users=10", 2_000_000, 10, 10},
}

// BenchmarkLargeFileRE
// BenchmarkLargeFileRE/payload=1KB/file=100MB/users=10
// BenchmarkLargeFileRE/payload=1KB/file=100MB/users=10-12         	      10	 108496904 ns/op	 880.94 MB/s	        93.91 peak-heap-MB	119556855 B/op	  158007 allocs/op
// BenchmarkLargeFileRE/payload=1KB/file=500MB/users=10
// BenchmarkLargeFileRE/payload=1KB/file=500MB/users=10-12         	       1	3001397083 ns/op	 159.51 MB/s	       472.0 peak-heap-MB	1532448960 B/op	 3399653 allocs/op
// BenchmarkLargeFileRE/payload=1KB/file=2GB/users=10
// BenchmarkLargeFileRE/payload=1KB/file=2GB/users=10-12           	       1	10531107333 ns/op	 182.08 MB/s	      1890 peak-heap-MB	6141554528 B/op	13599653 allocs/op
func BenchmarkLargeFileRE(b *testing.B) {
	ctx := context.Background()
	for _, sc := range largeFileScenarios {
		ids := makeSuppressedIDs(sc.numSuppressed)
		b.Run(sc.name, func(b *testing.B) {
			records, users := buildLargePayloadRecords(sc.numRecords, sc.suppressEvery, ids)
			b.SetBytes(int64(len(records)))
			b.ReportAllocs()

			peak := measurePeakHeap(func() {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentityRE(ctx, users); err != nil {
					b.Fatal(err)
				}
			})
			b.ReportMetric(float64(peak)/(1024*1024), "peak-heap-MB")

			for i := 0; i < b.N; i++ {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentityRE(ctx, users); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkLargeFilePureGo
// BenchmarkLargeFilePureGo/payload=1KB/file=100MB/users=10
// BenchmarkLargeFilePureGo/payload=1KB/file=100MB/users=10-12         	      78	  14604888 ns/op	6544.31 MB/s	        94.07 peak-heap-MB	101443087 B/op	  208716 allocs/op
// BenchmarkLargeFilePureGo/payload=1KB/file=500MB/users=10
// BenchmarkLargeFilePureGo/payload=1KB/file=500MB/users=10-12         	       1	1372806458 ns/op	 348.73 MB/s	       471.8 peak-heap-MB	1532283536 B/op	 4399542 allocs/op
// BenchmarkLargeFilePureGo/payload=1KB/file=2GB/users=10
// BenchmarkLargeFilePureGo/payload=1KB/file=2GB/users=10-12           	       1	8917455042 ns/op	 215.03 MB/s	      1897 peak-heap-MB	6155796264 B/op	17599557 allocs/op
func BenchmarkLargeFilePureGo(b *testing.B) {
	ctx := context.Background()
	for _, sc := range largeFileScenarios {
		ids := makeSuppressedIDs(sc.numSuppressed)
		b.Run(sc.name, func(b *testing.B) {
			records, users := buildLargePayloadRecords(sc.numRecords, sc.suppressEvery, ids)
			b.SetBytes(int64(len(records)))
			b.ReportAllocs()

			peak := measurePeakHeap(func() {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentityPureGo(ctx, users); err != nil {
					b.Fatal(err)
				}
			})
			b.ReportMetric(float64(peak)/(1024*1024), "peak-heap-MB")

			for i := 0; i < b.N; i++ {
				h := NewGZIPLocalFileHandler(SnakeCase)
				h.records = append(h.records[:0], records...)
				if err := h.RemoveIdentityPureGo(ctx, users); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
