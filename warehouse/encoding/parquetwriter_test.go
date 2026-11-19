package encoding

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestParquetWriterWideSchemaFlush(t *testing.T) {
	t.Setenv("RSERVER_WAREHOUSE_PARQUET_PAGE_SIZE", "1048576")
	conf := config.New()
	conf.Set("Warehouse.parquetParallelWriters", 8)
	factory := NewFactory(conf, logger.NOP)
	schema := make(model.TableSchema)
	row := make([]any, 100)
	for i := range row {
		schema[fmt.Sprintf("column_%03d", i)] = "string"
		row[i] = strings.Repeat("x", 1<<15)
	}
	path := t.TempDir() + "/wide.parquet"
	loadWriter, err := factory.NewLoadFileWriter(warehouseutils.LoadFileTypeParquet, path, schema, warehouseutils.RS)
	require.NoError(t, err)
	writer := loadWriter.(*parquetWriter)
	require.Equal(t, int64(maxParquetQueuedBytes), writer.maxQueuedBytes)
	for range 24 {
		require.NoError(t, writer.WriteRow(row))
	}
	// 24 rows * 100 columns * 32 KiB exceed the cap; the queue must drain
	// before Close even though the fork's interface-based estimate is tiny.
	require.Less(t, len(writer.writer.Objs), 24)
	require.Less(t, writer.queuedBytes, writer.maxQueuedBytes)
	require.NoError(t, writer.Close())
	file, err := local.NewLocalFileReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	pr, err := reader.NewParquetReader(file, nil, 1)
	require.NoError(t, err)
	defer pr.ReadStop()
	require.EqualValues(t, 24, pr.GetNumRows())
	require.NotEmpty(t, pr.Footer.RowGroups)
	rows := make([]any, 24)
	require.NoError(t, pr.Read(&rows))
	require.Len(t, rows, 24)
	for _, got := range rows {
		require.NotNil(t, got)
	}
}

func TestParquetWriterNarrowSchemaFlush(t *testing.T) {
	t.Setenv("RSERVER_WAREHOUSE_PARQUET_PAGE_SIZE", "1048576")
	conf := config.New()
	conf.Set("Warehouse.parquetParallelWriters", 8)
	factory := NewFactory(conf, logger.NOP)
	path := t.TempDir() + "/narrow.parquet"
	loadWriter, err := factory.NewLoadFileWriter(warehouseutils.LoadFileTypeParquet, path, model.TableSchema{
		"first": "string", "second": "string",
	}, warehouseutils.RS)
	require.NoError(t, err)
	writer := loadWriter.(*parquetWriter)
	require.Equal(t, int64(maxParquetQueuedBytes), writer.maxQueuedBytes)
	writer.maxQueuedBytes = 1 << 20 // Trigger the same flush path with a small test row.
	require.NoError(t, writer.WriteRow([]any{strings.Repeat("x", 1<<19), strings.Repeat("y", 1<<19)}))
	require.Empty(t, writer.writer.Objs)
	require.Zero(t, writer.queuedBytes)
	require.NoError(t, writer.Close())
}

func TestParquetWriterMillionRowsBoundedMemory(t *testing.T) {
	conf := config.New()
	conf.Set("Warehouse.parquetPageSize", 1<<20)
	factory := NewFactory(conf, logger.NOP)
	path := t.TempDir() + "/million-rows.parquet"
	loadWriter, err := factory.NewLoadFileWriter(warehouseutils.LoadFileTypeParquet, path, model.TableSchema{
		"first": "string", "second": "string",
	}, warehouseutils.RS)
	require.NoError(t, err)
	writer := loadWriter.(*parquetWriter)
	require.Equal(t, int64(maxParquetQueuedBytes), writer.maxQueuedBytes)

	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	baseline, peak := memory.HeapAlloc, memory.HeapAlloc
	valuePrefix := strings.Repeat("x", 256)
	for i := range 1_000_000 {
		value := valuePrefix + strconv.Itoa(i)
		if err := writer.WriteRow([]any{value, value}); err != nil {
			t.Fatal(err)
		}
		if i%1000 == 0 {
			runtime.ReadMemStats(&memory)
			peak = max(peak, memory.HeapAlloc)
		}
	}
	require.NoError(t, writer.Close())
	runtime.ReadMemStats(&memory)
	peak = max(peak, memory.HeapAlloc)
	// Without the queue cap, this narrow text write peaks above 600 MiB.
	require.Less(t, peak-baseline, uint64(384<<20), "peak heap growth should stay bounded with 1 MiB pages")
	t.Logf("peak heap growth for 1,000,000 rows: %.1f MiB", float64(peak-baseline)/(1<<20))

	file, err := local.NewLocalFileReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	pr, err := reader.NewParquetReader(file, nil, 1)
	require.NoError(t, err)
	defer pr.ReadStop()
	require.EqualValues(t, 1_000_000, pr.GetNumRows())
}
