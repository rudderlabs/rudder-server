package filehandler

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
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
// BenchmarkRemoveIdentityRE/records=1000/users=1/hit=10%-12         	    2467	    453139 ns/op	 302.32 MB/s	         0.6155 peak-heap-MB	  570112 B/op	    1050 allocs/op
// BenchmarkRemoveIdentityRE/records=1000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=1000/users=10/hit=10%-12        	    2689	    436403 ns/op	 313.91 MB/s	         0.6156 peak-heap-MB	  570273 B/op	    1053 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=1/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=1/hit=10%-12        	     259	   4330492 ns/op	 320.72 MB/s	         4.132 peak-heap-MB	 4433996 B/op	   10097 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=10/hit=10%-12       	     267	   4343849 ns/op	 319.74 MB/s	         4.170 peak-heap-MB	 4433766 B/op	   10099 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=100/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=100/hit=10%-12      	     262	   4417182 ns/op	 314.63 MB/s	         4.172 peak-heap-MB	 4436619 B/op	   10100 allocs/op
// BenchmarkRemoveIdentityRE/records=100000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=100000/users=10/hit=10%-12      	      24	  47840227 ns/op	 294.29 MB/s	        53.56 peak-heap-MB	61798302 B/op	  104239 allocs/op
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
// BenchmarkRemoveIdentityPureGo/records=1000/users=1/hit=10%-12         	    6938	    146518 ns/op	 934.98 MB/s	         0.5340 peak-heap-MB	  563187 B/op	    2014 allocs/op
// BenchmarkRemoveIdentityPureGo/records=1000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=1000/users=10/hit=10%-12        	    7092	    141906 ns/op	 965.36 MB/s	         0.5379 peak-heap-MB	  563640 B/op	    2017 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=1/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=1/hit=10%-12        	    1000	   1162690 ns/op	1194.55 MB/s	         4.079 peak-heap-MB	 4402641 B/op	   20037 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=10/hit=10%-12       	    1008	   1151765 ns/op	1205.88 MB/s	         3.425 peak-heap-MB	 4403062 B/op	   20039 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=100/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=100/hit=10%-12      	     939	   1254340 ns/op	1107.99 MB/s	         4.059 peak-heap-MB	 4406426 B/op	   20041 allocs/op
// BenchmarkRemoveIdentityPureGo/records=100000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=100000/users=10/hit=10%-12      	      93	  11219436 ns/op	1254.87 MB/s	        53.41 peak-heap-MB	59927303 B/op	  202175 allocs/op
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
