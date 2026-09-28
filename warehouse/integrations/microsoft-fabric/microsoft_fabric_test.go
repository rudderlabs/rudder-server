package microsoftfabric

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type testUploader struct{ canAppend bool }

func (*testUploader) IsWarehouseSchemaEmpty() bool { return false }
func (*testUploader) GetLocalSchema(context.Context) (model.Schema, error) {
	return model.Schema{}, nil
}
func (*testUploader) UpdateLocalSchema(context.Context, model.Schema) error { return nil }
func (*testUploader) GetTableSchemaInWarehouse(string) model.TableSchema    { return nil }
func (*testUploader) GetTableSchemaInUpload(string) model.TableSchema       { return nil }
func (*testUploader) GetLoadFilesMetadata(context.Context, warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	return nil, nil
}

func (*testUploader) GetSampleLoadFileLocation(context.Context, string) (string, error) {
	return "", nil
}

func (*testUploader) GetSingleLoadFile(context.Context, string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}
func (*testUploader) ShouldOnDedupUseNewRecord() bool { return false }
func (*testUploader) UseRudderStorage() bool          { return false }
func (*testUploader) GetLoadFileType() string         { return warehouseutils.LoadFileTypeParquet }
func (u *testUploader) CanAppend() bool               { return u.canAppend }

func TestTypeMappings(t *testing.T) {
	definitions, err := columnsWithTypes(model.TableSchema{
		"active": "boolean", "count": "int", "price": "float", "received_at": "datetime", "payload": "json", "name": "string",
	})
	require.NoError(t, err)
	require.Contains(t, definitions, "[active] bit")
	require.Contains(t, definitions, "[price] decimal(28,10)")
	require.Contains(t, definitions, "[received_at] datetime2(6)")
	require.Contains(t, definitions, "[payload] varchar(max)")
}

func TestShouldMerge(t *testing.T) {
	fabric := New(config.New(), logger.NOP, stats.Default)
	fabric.uploader = &testUploader{canAppend: true}
	fabric.warehouse = model.Warehouse{Type: provider, Destination: backendconfig.DestinationT{Config: map[string]any{"preferAppend": true}}}
	require.False(t, fabric.ShouldMerge("tracks"))
	require.True(t, fabric.ShouldMerge(warehouseutils.UsersTable))

	fabric.uploader = &testUploader{canAppend: false}
	require.True(t, fabric.ShouldMerge("tracks"))

	fabric.uploader = &testUploader{canAppend: true}
	fabric.warehouse.Destination.Config["preferAppend"] = false
	require.True(t, fabric.ShouldMerge("tracks"))
}

func TestCopyIntoUsesExplicitColumnsAndParquet(t *testing.T) {
	statement := copyIntoStatement("[schema].[tracks]", []string{"[id]", "[name]"}, "https://host/workspace/lakehouse.Lakehouse/Files/load.parquet")
	require.Equal(t, "COPY INTO [schema].[tracks] ([id],[name]) FROM 'https://host/workspace/lakehouse.Lakehouse/Files/load.parquet' WITH (FILE_TYPE = 'PARQUET');", statement)
}

func TestMergeDeduplicatesByPrimaryKey(t *testing.T) {
	statement := mergeStatement("[schema].[tracks]", "[schema].[staging]", []string{"id", "name", "received_at"}, []string{"id"})
	require.Contains(t, statement, "ROW_NUMBER() OVER (PARTITION BY [id] ORDER BY [received_at] DESC)")
	require.Contains(t, statement, "target.[id] = source.[id]")
	require.Contains(t, statement, "WHEN MATCHED THEN UPDATE SET target.[name] = source.[name]")
	require.Contains(t, statement, "WHEN NOT MATCHED THEN INSERT")
}
