package encoding

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMicrosoftFabricParquetWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fabric.parquet")
	schema := model.TableSchema{
		"active":      "boolean",
		"amount":      "float",
		"count":       "int",
		"event_json":  "json",
		"name":        "string",
		"received_at": "datetime",
	}
	factory := NewFactory(config.New())
	writer, err := factory.NewLoadFileWriter(warehouseutils.LoadFileTypeParquet, path, schema, warehouseutils.MicrosoftFabric)
	require.NoError(t, err)
	loader := factory.NewEventLoader(writer, warehouseutils.LoadFileTypeParquet, warehouseutils.MicrosoftFabric)
	loader.AddColumn("active", "boolean", true)
	loader.AddColumn("amount", "float", 1.25)
	loader.AddColumn("count", "int", 7)
	loader.AddColumn("event_json", "json", `{"key":"value"}`)
	loader.AddColumn("name", "string", "Fabric")
	loader.AddColumn("received_at", "datetime", "2025-01-02T03:04:05.123456Z")
	require.NoError(t, loader.Write())
	require.NoError(t, writer.Close())

	file, err := local.NewLocalFileReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	parquetReader, err := reader.NewParquetReader(file, nil, 1)
	require.NoError(t, err)
	defer parquetReader.ReadStop()
	columnNames := make([]string, 0, len(parquetReader.SchemaHandler.ValueColumns))
	for _, columnPath := range parquetReader.SchemaHandler.ValueColumns {
		parts := strings.Split(columnPath, "\x01")
		columnNames = append(columnNames, parts[len(parts)-1])
	}
	require.Equal(t, []string{"Active", "Amount", "Count", "Event_json", "Name", "Received_at"}, columnNames)

	type fabricRow struct {
		Active      *bool
		Amount      *float64
		Count       *int64
		Event_json  *string
		Name        *string
		Received_at *int64
	}
	rows := make([]*fabricRow, 1)
	require.NoError(t, parquetReader.Read(&rows))
	require.Equal(t, true, *rows[0].Active)
	require.Equal(t, 1.25, *rows[0].Amount)
	require.Equal(t, int64(7), *rows[0].Count)
	require.Equal(t, `{"key":"value"}`, *rows[0].Event_json)
	require.Equal(t, "Fabric", *rows[0].Name)
	require.Equal(t, int64(1735787045123456), *rows[0].Received_at)
}

func TestMicrosoftFabricParquetRejectsUnknownType(t *testing.T) {
	_, err := NewFactory(config.New()).NewLoadFileWriter(
		warehouseutils.LoadFileTypeParquet,
		filepath.Join(t.TempDir(), "fabric.parquet"),
		model.TableSchema{"bad": "array"},
		warehouseutils.MicrosoftFabric,
	)
	require.ErrorContains(t, err, "unsupported data type")
}
