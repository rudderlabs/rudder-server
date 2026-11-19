package encoding

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestParquetWriterLoadFileMemory(t *testing.T) {
	const rowCount = 80_000
	const pageSizeEnv = "RUDDER_PARQUET_MEMORY_TEST_PAGE_SIZE"
	const peakPrefix = "PARQUET_PEAK_HEAP_BYTES="

	if pageSizeText := os.Getenv(pageSizeEnv); pageSizeText != "" {
		pageSize, err := strconv.ParseInt(pageSizeText, 10, 64)
		require.NoError(t, err)
		fmt.Printf("%s%d\n", peakPrefix, writeParquetMemoryTestFile(t, rowCount, pageSize))
		return
	}

	measure := func(pageSize int64) uint64 {
		t.Helper()
		// Use a fresh process for each page size so prior runs do not change GC pacing.
		cmd := exec.Command(os.Args[0], "-test.run=^TestParquetWriterLoadFileMemory$")
		cmd.Env = append(os.Environ(), pageSizeEnv+"="+strconv.FormatInt(pageSize, 10))
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		for line := range strings.SplitSeq(string(output), "\n") {
			if value, ok := strings.CutPrefix(line, peakPrefix); ok {
				peak, err := strconv.ParseUint(value, 10, 64)
				require.NoError(t, err)
				return peak
			}
		}
		t.Fatalf("peak heap metric missing from test output: %s", output)
		return 0
	}

	largePagePeak := measure(1 << 20)
	defaultPagePeak := measure(defaultParquetPageSize)
	t.Logf("peak heap growth for %d rows: 1 MiB pages %.1f MiB, 8 KiB pages %.1f MiB", rowCount,
		float64(largePagePeak)/(1<<20), float64(defaultPagePeak)/(1<<20))
	require.LessOrEqual(t, largePagePeak, defaultPagePeak)
}

func writeParquetMemoryTestFile(t *testing.T, rowCount int, pageSize int64) uint64 {
	t.Helper()
	// Two JSON text columns total about 512 MB of raw row data.
	keys := make([]string, 144)
	for i := range keys {
		keys[i] = `"property_` + strconv.Itoa(i) + `":"`
	}
	// Reuse six event shapes across rows, while keeping each event ID distinct.
	eventJSON := func(row, fields int) string {
		var b strings.Builder
		b.Grow(fields * 32)
		b.WriteByte('{')
		b.WriteString(`"row":`)
		b.WriteString(strconv.Itoa(row))
		for i, key := range keys[:fields] {
			b.WriteByte(',')
			b.WriteString(key)
			value := uint64(row%6+1)*0x9e3779b97f4a7c15 ^ uint64(i+1)*0xbf58476d1ce4e5b9
			b.WriteString(strconv.FormatUint(value|1<<60, 16))
			b.WriteByte('"')
		}
		b.WriteByte('}')
		return b.String()
	}

	conf := config.New()
	conf.Set("Warehouse.parquetPageSize", pageSize)
	factory := NewFactory(conf)
	// Build the same-sized input before measuring so fixture allocation is not counted as writer growth.
	rows := make([][]any, rowCount)
	rawBytes := 0
	for i := range rows {
		first, second := eventJSON(i, 144), eventJSON(i+3, 48)
		rows[i] = []any{first, second}
		rawBytes += len(first) + len(second)
	}
	require.InDelta(t, 512_000_000, rawBytes, 2_000_000)
	path := t.TempDir() + "/events.parquet"
	writer, err := factory.NewLoadFileWriter(warehouseutils.LoadFileTypeParquet, path, model.TableSchema{
		"first": "string", "second": "string",
	}, warehouseutils.GCSDatalake)
	require.NoError(t, err)

	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	baseline, peak := memory.HeapAlloc, memory.HeapAlloc
	for i, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatal(err)
		}
		if i%250 == 0 {
			runtime.ReadMemStats(&memory)
			peak = max(peak, memory.HeapAlloc)
		}
	}
	require.NoError(t, writer.Close())
	runtime.ReadMemStats(&memory)
	peak = max(peak, memory.HeapAlloc)

	file, err := local.NewLocalFileReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	pr, err := reader.NewParquetReader(file, nil, 1)
	require.NoError(t, err)
	defer pr.ReadStop()
	require.EqualValues(t, rowCount, pr.GetNumRows())
	return peak - baseline
}
