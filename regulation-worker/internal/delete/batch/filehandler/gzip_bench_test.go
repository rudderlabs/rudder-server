package filehandler

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

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
// BenchmarkRemoveIdentity/records=1000/users=1/hit=10%-12         	     201	   6043276 ns/op	  22.67 MB/s	  413003 B/op	     103 allocs/op
// BenchmarkRemoveIdentity/records=1000/users=10/hit=10%
// BenchmarkRemoveIdentity/records=1000/users=10/hit=10%-12        	      68	  17094042 ns/op	   8.01 MB/s	  414696 B/op	     122 allocs/op
// BenchmarkRemoveIdentity/records=10000/users=1/hit=10%
// BenchmarkRemoveIdentity/records=10000/users=1/hit=10%-12        	      58	  19767681 ns/op	  70.26 MB/s	 5599990 B/op	     110 allocs/op
// BenchmarkRemoveIdentity/records=10000/users=10/hit=10%
// BenchmarkRemoveIdentity/records=10000/users=10/hit=10%-12       	       8	 126603115 ns/op	  10.97 MB/s	 5601873 B/op	     130 allocs/op
// BenchmarkRemoveIdentity/records=10000/users=100/hit=10%
// BenchmarkRemoveIdentity/records=10000/users=100/hit=10%-12      	       1	1259558500 ns/op	   1.10 MB/s	 5624544 B/op	     313 allocs/op
// BenchmarkRemoveIdentity/records=100000/users=10/hit=10%
// BenchmarkRemoveIdentity/records=100000/users=10/hit=10%-12      	       1	1223033209 ns/op	  11.51 MB/s	47651936 B/op	     136 allocs/op
func BenchmarkRemoveIdentity(b *testing.B) {
	ctx := context.Background()
	for _, sc := range benchScenarios {
		ids := makeSuppressedIDs(sc.numSuppressed)
		records, users := buildBenchRecords(sc.numRecords, sc.suppressEvery, ids)
		b.Run(sc.name, func(b *testing.B) {
			b.SetBytes(int64(len(records)))
			b.ReportAllocs()
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
// BenchmarkRemoveIdentityRE/records=1000/users=1/hit=10%-12         	    2610	    451851 ns/op	 303.18 MB/s	  570800 B/op	    1050 allocs/op
// BenchmarkRemoveIdentityRE/records=1000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=1000/users=10/hit=10%-12        	    2666	    473797 ns/op	 289.13 MB/s	  571068 B/op	    1053 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=1/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=1/hit=10%-12        	     265	   4403228 ns/op	 315.43 MB/s	 4415194 B/op	   10058 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=10/hit=10%-12       	     271	   4394131 ns/op	 316.08 MB/s	 4416410 B/op	   10061 allocs/op
// BenchmarkRemoveIdentityRE/records=10000/users=100/hit=10%
// BenchmarkRemoveIdentityRE/records=10000/users=100/hit=10%-12      	     267	   4425025 ns/op	 314.08 MB/s	 4419899 B/op	   10061 allocs/op
// BenchmarkRemoveIdentityRE/records=100000/users=10/hit=10%
// BenchmarkRemoveIdentityRE/records=100000/users=10/hit=10%-12      	      24	  44131311 ns/op	 319.02 MB/s	59318300 B/op	  100068 allocs/op
func BenchmarkRemoveIdentityRE(b *testing.B) {
	ctx := context.Background()
	for _, sc := range benchScenarios {
		ids := makeSuppressedIDs(sc.numSuppressed)
		records, users := buildBenchRecords(sc.numRecords, sc.suppressEvery, ids)
		b.Run(sc.name, func(b *testing.B) {
			b.SetBytes(int64(len(records)))
			b.ReportAllocs()
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
// BenchmarkRemoveIdentityPureGo/records=1000/users=1/hit=10%-12         	    7198	    151627 ns/op	 903.47 MB/s	  563106 B/op	    2014 allocs/op
// BenchmarkRemoveIdentityPureGo/records=1000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=1000/users=10/hit=10%-12        	    7791	    140989 ns/op	 971.64 MB/s	  563561 B/op	    2017 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=1/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=1/hit=10%-12        	     981	   1211181 ns/op	1146.72 MB/s	 4398249 B/op	   20017 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=10/hit=10%-12       	     987	   1169327 ns/op	1187.77 MB/s	 4398699 B/op	   20020 allocs/op
// BenchmarkRemoveIdentityPureGo/records=10000/users=100/hit=10%
// BenchmarkRemoveIdentityPureGo/records=10000/users=100/hit=10%-12      	     960	   1255105 ns/op	1107.31 MB/s	 4401739 B/op	   20020 allocs/op
// BenchmarkRemoveIdentityPureGo/records=100000/users=10/hit=10%
// BenchmarkRemoveIdentityPureGo/records=100000/users=10/hit=10%-12      	     106	  11176147 ns/op	1259.73 MB/s	59289715 B/op	  200024 allocs/op
func BenchmarkRemoveIdentityPureGo(b *testing.B) {
	ctx := context.Background()
	for _, sc := range benchScenarios {
		ids := makeSuppressedIDs(sc.numSuppressed)
		records, users := buildBenchRecords(sc.numRecords, sc.suppressEvery, ids)
		b.Run(sc.name, func(b *testing.B) {
			b.SetBytes(int64(len(records)))
			b.ReportAllocs()
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
