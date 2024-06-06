package helper_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/helper/buffer"
	fh "github.com/rudderlabs/rudder-server/helper/file"
	"github.com/rudderlabs/rudder-server/helper/types"
	"github.com/stretchr/testify/require"
)

type fhBenchmarkConfig struct {
	nEvents      int64
	batchSize    int
	batchTimeout time.Duration
	fileHandle   *fh.FileHandle
}

func (bc *fhBenchmarkConfig) makeLoad() {
	inp := `{"request":{"url": "http://website.com","headers":{"Content-type":"application/json"}}}`
	out := `{"response":{"statusCode":200,"body":{"users":[{"id":1,"name":"patrick"},{"id":2,"name":"jane"}]}}}`
	metaInf := types.MetaInfo{
		WorkspaceID:   "wsp",
		DestinationID: "d1",
		DestType:      "DEST_TYPE",
		EventName:     "bench",
	}
	for i := 0; i < int(bc.nEvents); i++ {
		bc.fileHandle.Send(inp, out, metaInf)
	}
}

func BenchmarkFileHelper(pb *testing.B) {
	configs := []fhBenchmarkConfig{
		{
			nEvents:      10,
			batchSize:    5,
			batchTimeout: 2 * time.Second,
		},
		// {
		// 	nEvents: 1000,
		// 	batchSize: 500,
		// 	batchTimeout: 20*time.Second,
		// },
		// {
		// 	nEvents: int64(math.Pow10(5)),
		// 	batchSize: int(math.Pow10(5)/2),
		// 	batchTimeout: 5*time.Minute,
		// },
		// {
		// 	nEvents: int64(math.Pow10(8)),
		// 	batchSize: int(math.Pow10(8)/2),
		// 	batchTimeout: 10*time.Minute,
		// },
	}
	conf := config.New()
	for _, cfg := range configs {
		conf.Set("some.DebugHelper.maxBatchSize", cfg.batchSize)
		conf.Set("some.DebugHelper.maxBatchTimeout", cfg.batchTimeout)

		h, err := fh.New(pb.TempDir(), fh.WithOptsFromConfig("some", conf))
		require.NoError(pb, err)
		cfg.fileHandle = h

		pbStr := fmt.Sprintf("bench for n=%d", cfg.nEvents)
		pb.Run(pbStr, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cfg.makeLoad()
			}
			// b.Cleanup(cfg.fileHandle.Shutdown)
		})
	}
}

type bhBenchmarkConfig struct {
	nEvents                 int64
	bufferCapacityInB       int
	maxBytesForFileRotation int
	bh                      *buffer.BufferHandle
}

func (bc *bhBenchmarkConfig) makeLoad() {
	inp := `{"request":{"url": "http://website.com","headers":{"Content-type":"application/json"}}}`
	out := `{"response":{"statusCode":200,"body":{"users":[{"id":1,"name":"patrick"},{"id":2,"name":"jane"}]}}}`
	metaInf := types.MetaInfo{
		WorkspaceID:   "wsp",
		DestinationID: "d1",
		DestType:      "DEST_TYPE",
		EventName:     "bench",
	}
	for i := 0; i < int(bc.nEvents); i++ {
		bc.bh.Send(inp, out, metaInf)
	}
}
// BenchmarkBufferIOFileHelper/bench_for_n=10-12         	  157378	      7526 ns/op
// BenchmarkBufferIOFileHelper/bench_for_n=100-12        	   16298	     74468 ns/op
func BenchmarkBufferIOFileHelper(pb *testing.B) {
	configs := []bhBenchmarkConfig{
		{
			nEvents:                 10,
			bufferCapacityInB:       4*units.MiB,
			maxBytesForFileRotation: int(40 * units.MiB),
		},
		{
			nEvents:                 100,
			bufferCapacityInB:       40*units.MiB,
			maxBytesForFileRotation: int(100 * units.MiB),
		},
	}
	conf := config.New()
	for _, cfg := range configs {
		conf.Set("some.DebugHelper.bufferCapacityInB", cfg.bufferCapacityInB)
		conf.Set("some.DebugHelper.maxBytesForFileRotation", cfg.maxBytesForFileRotation)
		
		h := buffer.New(pb.TempDir() + strconv.Itoa(int(cfg.nEvents)) + "/", buffer.WithOptsFromConfig("some", conf))
		cfg.bh = h
		pbStr := fmt.Sprintf("bench for n=%d", cfg.nEvents)
		pb.Run(pbStr, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cfg.makeLoad()
			}
		})
		pb.Cleanup(cfg.bh.Shutdown)
	}
}
