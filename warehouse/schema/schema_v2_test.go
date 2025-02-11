package schema

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockFetchSchemaRepoV2 struct {
	count int
}

func (m *mockFetchSchemaRepoV2) FetchSchema(ctx context.Context) (model.Schema, error) {
	m.count++
	schema := model.Schema{
		"table1": {
			"column1": "string",
			"column2": "int",
		},
		"table2": {
			"column11": "string",
			"column12": "int",
			"column13": "int",
		},
	}
	if m.count == 1 {
		return schema, nil
	}
	schema["table2"]["column14"] = "float"
	return schema, nil
}

func TestSchemaV2(t *testing.T) {
	warehouse := model.Warehouse{
		Destination: backendconfig.DestinationT{
			ID: "dest_id",
		},
		Namespace: "namespace",
		Source: backendconfig.SourceT{
			ID: "source_id",
		},
	}
	v1 := &schema{
		schemaRepo: &mockSchemaRepo{
			schemaMap: make(map[string]model.WHSchema),
		},
	}
	ttl := 10 * time.Minute
	v2 := newSchemaV2(v1, warehouse, logger.NOP, ttl, &mockFetchSchemaRepoV2{})
	ctx := context.Background()

	t.Run("SyncRemoteSchema", func(t *testing.T) {
		schema, err := v2.GetLocalSchema(ctx)
		require.NoError(t, err)
		require.Equal(t, model.Schema{}, schema)
		require.True(t, v2.IsWarehouseSchemaEmpty(ctx))

		_, err = v2.SyncRemoteSchema(ctx, nil, 0)
		require.NoError(t, err)
		schema, err = v2.GetLocalSchema(ctx)
		require.NoError(t, err)
		require.Equal(t, model.Schema{
			"table1": {
				"column1": "string",
				"column2": "int",
			},
			"table2": {
				"column11": "string",
				"column12": "int",
				"column13": "int",
			},
		}, schema)
	})

	t.Run("Test ttl", func(t *testing.T) {
		v1 := &schema{
			schemaRepo: &mockSchemaRepo{
				schemaMap: map[string]model.WHSchema{
					"source_id_dest_id_namespace": {
						Schema: model.Schema{
							"table1": {
								"column1": "string",
								"column2": "int",
							},
							"table2": {
								"column11": "string",
							},
						},
						ExpiresAt: timeutil.Now().Add(10 * time.Minute),
					},
				},
			},
		}
		v2 := newSchemaV2(v1, warehouse, logger.NOP, ttl, &mockFetchSchemaRepoV2{})
		count, err := v2.GetColumnsCountInWarehouseSchema(ctx, "table2")
		require.NoError(t, err)
		require.Equal(t, 1, count)
		_, err = v2.SyncRemoteSchema(ctx, nil, 0)
		require.NoError(t, err)
		count, err = v2.GetColumnsCountInWarehouseSchema(ctx, "table2")
		require.NoError(t, err)
		require.Equal(t, 3, count)

		v2.now = func() time.Time {
			return timeutil.Now().Add(ttl * 2)
		}
		count, err = v2.GetColumnsCountInWarehouseSchema(ctx, "table2")
		require.NoError(t, err)
		require.Equal(t, 4, count)
	})

	t.Run("TableSchemaDiff", func(t *testing.T) {
		err := v2.UpdateWarehouseTableSchema(ctx, "table2", model.TableSchema{
			"column11": "string",
			"column12": "int",
			"column13": "int",
			"column14": "string",
		})
		require.NoError(t, err)
		diff, err := v2.TableSchemaDiff(ctx, "table2", model.TableSchema{
			"column11": "float",
			"column15": "int",
		})
		require.NoError(t, err)
		require.Equal(t, whutils.TableSchemaDiff{
			Exists:           true,
			TableToBeCreated: false,
			ColumnMap: model.TableSchema{
				"column15": "int",
			},
			UpdatedSchema:    model.TableSchema{"column11": "string", "column12": "int", "column13": "int", "column14": "string", "column15": "int"},
			AlteredColumnMap: model.TableSchema{},
		}, diff)
	})
}
