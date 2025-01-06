package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type mFetchSchemaRepo struct{}

var schema1 = model.Schema{
	"identifies": model.TableSchema{
		"id":      "string",
		"user_id": "int",
	},
}

var warehouse = model.Warehouse{
	WorkspaceID: "workspaceID",
	Source: backendconfig.SourceT{
		ID: "sourceID",
	},
	Destination: backendconfig.DestinationT{
		ID: "destinationID",
	},
	Namespace: "namespace",
	Type:      "warehouseType",
}

func (m *mFetchSchemaRepo) FetchSchema(ctx context.Context) (model.Schema, error) {
	return schema1, nil
}

type mSchemaRepo struct {
	schemaMap map[string]model.WHSchema
}

func (m *mSchemaRepo) Insert(ctx context.Context, whSchema *model.WHSchema) (int64, error) {
	if m.schemaMap == nil {
		m.schemaMap = make(map[string]model.WHSchema)
	}
	m.schemaMap[whSchema.Namespace] = *whSchema
	return 0, nil
}

func (m *mSchemaRepo) GetForNamespace(ctx context.Context, sourceID, destinationID, namespace string) (model.WHSchema, error) {
	return m.schemaMap[namespace], nil
}

func TestFetchAndSaveSchema(t *testing.T) {
	schemaRepo := &mSchemaRepo{}
	v1 := &schema{
		schemaRepo: schemaRepo,
	}
	ctx := context.Background()
	v2 := newSchemaV2(ctx, v1, warehouse, nil, stats.NOP.NewStat("schema_size", "size of the schema"))
	isEmpty := v2.IsWarehouseSchemaEmpty()
	require.True(t, isEmpty)

	fetchSchemaRepo := &mFetchSchemaRepo{}

	err := FetchAndSaveSchema(ctx, fetchSchemaRepo, warehouse, schemaRepo, nil)
	require.NoError(t, err)

	schema, err := v2.GetLocalSchema(ctx)
	require.NoError(t, err)
	require.Equal(t, model.Schema{}, schema)

	hasSchemaChanged, err := v2.SyncRemoteSchema(ctx, fetchSchemaRepo, 0)
	require.NoError(t, err)
	require.False(t, hasSchemaChanged)

	schema, err = v2.GetLocalSchema(ctx)
	require.NoError(t, err)
	require.Equal(t, schema1, schema)
}

func TestUpdateLocalSchema(t *testing.T) {
	schemaRepo := &mSchemaRepo{}
	v1 := &schema{
		schemaRepo: schemaRepo,
	}
	ctx := context.Background()
	v2 := newSchemaV2(ctx, v1, warehouse, nil, stats.NOP.NewStat("schema_size", "size of the schema"))

	schema, err := v2.GetLocalSchema(ctx)
	require.NoError(t, err)
	require.Equal(t, model.Schema{}, schema)

	schema2 := model.Schema{
		"users": model.TableSchema{
			"anonymous_id": "string",
			"received_at":  "datetime",
		},
	}

	err = v2.UpdateLocalSchema(ctx, schema2)
	require.NoError(t, err)

	schema, err = v2.GetLocalSchema(ctx)
	require.NoError(t, err)
	require.Equal(t, schema2, schema)
}
