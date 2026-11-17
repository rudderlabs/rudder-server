package encoding

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestParquetWriterWideSchemaFlush(t *testing.T) {
	t.Setenv("RSERVER_WAREHOUSE_PARQUET_PAGE_SIZE", "1048576")
	t.Setenv("RSERVER_WAREHOUSE_PARQUET_COMPRESSION", "ZSTD")
	conf := config.New()
	conf.Set("Warehouse.parquetParallelWriters", 8)
	factory := NewFactory(conf)
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
