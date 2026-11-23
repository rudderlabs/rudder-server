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
	const pageSizeEnv = "RUDDER_PARQUET_MEMORY_TEST_PAGE_SIZE"
	const shapeEnv = "RUDDER_PARQUET_MEMORY_TEST_SHAPE"
	const livePrefix = "PARQUET_LIVE_HEAP_BYTES="

	if pageSizeText := os.Getenv(pageSizeEnv); pageSizeText != "" {
		pageSize, err := strconv.ParseInt(pageSizeText, 10, 64)
		require.NoError(t, err)
		live := writeParquetMemoryTestFile(t, os.Getenv(shapeEnv), pageSize)
		fmt.Printf("%s%d,%d\n", livePrefix, live[0], live[1])
		return
	}
	if os.Getenv("SLOW") != "1" {
		t.Skip("set SLOW=1 to run the parquet load-file memory test")
	}

	measure := func(shape string, pageSize int64) [2]uint64 {
		t.Helper()
		// Use a fresh process for each page size so prior runs do not change GC pacing.
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestParquetWriterLoadFileMemory$")
		cmd.Env = append(os.Environ(), pageSizeEnv+"="+strconv.FormatInt(pageSize, 10), shapeEnv+"="+shape)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		for line := range strings.SplitSeq(string(output), "\n") {
			if value, ok := strings.CutPrefix(line, livePrefix); ok {
				first, second, ok := strings.Cut(value, ",")
				require.True(t, ok, "invalid live heap metric: %q", value)
				firstBytes, err := strconv.ParseUint(first, 10, 64)
				require.NoError(t, err)
				secondBytes, err := strconv.ParseUint(second, 10, 64)
				require.NoError(t, err)
				return [2]uint64{firstBytes, secondBytes}
			}
		}
		t.Fatalf("live heap metric missing from test output: %s", output)
		return [2]uint64{}
	}

	for _, shape := range []string{"two JSON columns", "wide schema"} {
		t.Run(shape, func(t *testing.T) {
			largePageLive := measure(shape, 1<<20)
			defaultPageLive := measure(shape, defaultParquetPageSize)
			t.Logf("live heap growth (first, second): 1 MiB pages %.1f, %.1f MiB; 8 KiB pages %.1f, %.1f MiB",
				float64(largePageLive[0])/(1<<20), float64(largePageLive[1])/(1<<20),
				float64(defaultPageLive[0])/(1<<20), float64(defaultPageLive[1])/(1<<20))
			for i := range largePageLive {
				require.LessOrEqual(t, largePageLive[i], defaultPageLive[i])
			}
			require.LessOrEqual(t, largePageLive[1], largePageLive[0]+uint64(maxParquetQueuedBytes),
				"live heap should stay within one queue cap as the row count doubles")
		})
	}
}

func writeParquetMemoryTestFile(t *testing.T, shape string, pageSize int64) [2]uint64 {
	t.Helper()
	// Two JSON text columns total about 506 MB of raw row data at 85k rows.
	keys := make([]string, 134)
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
			shapeID := row % 6
			if i < 10 {
				shapeID = row // Each event also has changing identifier fields.
			}
			value := uint64(shapeID+1)*0x9e3779b97f4a7c15 ^ uint64(i+1)*0xbf58476d1ce4e5b9
			b.WriteString(strconv.FormatUint(value|1<<60, 16))
			b.WriteByte('"')
		}
		b.WriteByte('}')
		return b.String()
	}

	conf := config.New()
	conf.Set("Warehouse.parquetPageSize", pageSize)
	factory := NewFactory(conf)
	var schema model.TableSchema
	var checkpoints [2]int
	switch shape {
	case "two JSON columns":
		schema = model.TableSchema{"first": "string", "second": "string"}
		checkpoints = [2]int{42_500, 85_000}
	case "wide schema":
		schema = make(model.TableSchema, 64)
		for i := range 64 {
			schema[fmt.Sprintf("column_%03d", i)] = "string"
		}
		checkpoints = [2]int{4_000, 8_000}
	default:
		t.Fatalf("unknown memory test shape %q", shape)
	}
	path := t.TempDir() + "/events.parquet"
	writer, err := factory.NewLoadFileWriter(warehouseutils.LoadFileTypeParquet, path, schema, warehouseutils.GCSDatalake)
	require.NoError(t, err)

	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	baseline := memory.HeapAlloc
	var live [2]uint64
	rawBytes := 0
	widePayload := strings.Repeat("x", 500)
	for i := range checkpoints[1] {
		var row []any
		switch shape {
		case "two JSON columns":
			first, second := eventJSON(i, 134), eventJSON(i+3, 45)
			row = []any{first, second}
			rawBytes += len(first) + len(second)
		case "wide schema":
			row = make([]any, 64)
			rowID := strconv.Itoa(i)
			for column := range row {
				row[column] = widePayload + rowID + ":" + strconv.Itoa(column)
			}
		}
		if err := writer.WriteRow(row); err != nil {
			t.Fatal(err)
		}
		for checkpoint, count := range checkpoints {
			if i+1 != count {
				continue
			}
			runtime.GC()
			runtime.ReadMemStats(&memory)
			live[checkpoint] = memory.HeapAlloc - baseline
		}
	}
	require.NoError(t, writer.Close())
	if shape == "two JSON columns" {
		require.InDelta(t, 506_000_000, rawBytes, 2_000_000)
	}

	file, err := local.NewLocalFileReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	pr, err := reader.NewParquetReader(file, nil, 1)
	require.NoError(t, err)
	defer pr.ReadStop()
	require.EqualValues(t, checkpoints[1], pr.GetNumRows())
	for read := 0; read < checkpoints[1]; {
		rows, err := pr.ReadByNumber(min(1_000, checkpoints[1]-read))
		require.NoError(t, err)
		require.NotEmpty(t, rows)
		read += len(rows)
	}
	return live
}
