package schema

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/samber/lo"

	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func schemaKey(destinationID, namespace string) string {
	return fmt.Sprintf("%s_%s", destinationID, namespace)
}

type mockSchemaRepo struct {
	schemaMap map[string]model.WHSchema
	mu        sync.RWMutex
}

func (m *mockSchemaRepo) GetForNamespace(_ context.Context, destinationID, namespace string) (model.WHSchema, error) {
	key := schemaKey(destinationID, namespace)

	m.mu.RLock()
	schema := m.schemaMap[key]
	m.mu.RUnlock()

	return schema, nil
}

func (m *mockSchemaRepo) Insert(_ context.Context, schema *model.WHSchema) error {
	key := schemaKey(schema.DestinationID, schema.Namespace)

	m.mu.Lock()
	// This is to simulate the logic in the underlying sql repo.
	if schema.ExpiresAt.IsZero() {
		schema.ExpiresAt = m.schemaMap[key].ExpiresAt
	}
	m.schemaMap[key] = *schema
	m.mu.Unlock()

	return nil
}

type mockFetchSchemaRepo struct {
	err error
}

func (m *mockFetchSchemaRepo) FetchSchema(ctx context.Context) (model.Schema, error) {
	if m.err != nil {
		return nil, m.err
	}
	return model.Schema{
		"table1": {
			"column1": "string",
			"column2": "int",
		},
		"table2": {
			"column11": "string",
			"column12": "int",
			"column13": "int",
		},
	}, nil
}

type mockStagingFileRepo struct {
	schemas []model.Schema
	err     error
}

func (m *mockStagingFileRepo) GetSchemasByIDs(context.Context, []int64) ([]model.Schema, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.schemas, nil
}

func newSchema(t *testing.T, warehouse model.Warehouse, schemaRepo schemaRepo) Handler {
	t.Helper()
	sh, err := New(context.Background(), warehouse, config.New(), logger.NOP, stats.NOP, &mockFetchSchemaRepo{}, schemaRepo, nil)
	require.NoError(t, err)
	return sh
}

func TestSchema(t *testing.T) {
	warehouse := model.Warehouse{
		Destination: backendconfig.DestinationT{
			ID: "dest_id",
		},
		Namespace: "namespace",
		Source: backendconfig.SourceT{
			ID: "source_id",
		},
	}
	sch := newSchema(t, warehouse, &mockSchemaRepo{
		schemaMap: make(map[string]model.WHSchema),
	})
	ctx := context.Background()

	t.Run("IsSchemaEmpty", func(t *testing.T) {
		sch := newSchema(t, warehouse, &mockSchemaRepo{
			schemaMap: map[string]model.WHSchema{
				"dest_id_namespace": {
					Schema:    model.Schema{},
					ExpiresAt: timeutil.Now().Add(10 * time.Minute),
				},
			},
		})
		require.True(t, sch.IsSchemaEmpty(ctx))
		err := sch.UpdateSchema(ctx, model.Schema{
			"table1": {
				"column1": "string",
				"column2": "int",
			},
			"table2": {
				"column11": "string",
			},
		})
		require.NoError(t, err)
		require.False(t, sch.IsSchemaEmpty(ctx))
	})

	t.Run("GetTableSchema", func(t *testing.T) {
		sch := newSchema(t, warehouse, &mockSchemaRepo{
			schemaMap: map[string]model.WHSchema{
				"dest_id_namespace": {
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
		})
		tableSchema := sch.GetTableSchema(ctx, "table1")
		require.Equal(t, model.TableSchema{
			"column1": "string",
			"column2": "int",
		}, tableSchema)
	})

	t.Run("ExpiredSchema", func(t *testing.T) {
		sch := newSchema(t, warehouse, &mockSchemaRepo{
			schemaMap: map[string]model.WHSchema{
				"dest_id_namespace": {
					Schema: model.Schema{
						"table1": {
							"column1": "string",
							"column2": "int",
						},
						"table2": {
							"column11": "string",
						},
					},
					ExpiresAt: timeutil.Now().Add(-10 * time.Minute),
				},
			},
		})
		count, err := sch.GetColumnsCount(ctx, "table2")
		require.NoError(t, err)
		require.Equal(t, 3, count)
	})

	t.Run("TableSchemaDiff", func(t *testing.T) {
		err := sch.UpdateTableSchema(ctx, "table2", model.TableSchema{
			"column11": "string",
			"column12": "int",
			"column13": "int",
			"column14": "string",
		})
		require.NoError(t, err)
		diff, err := sch.TableSchemaDiff(ctx, "table2", model.TableSchema{
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

	t.Run("ConsolidateStagingFilesSchema", func(t *testing.T) {
		sourceID := "test_source_id"
		destinationID := "test_destination_id"
		namespace := "test_namespace"
		destType := whutils.RS
		workspaceID := "test-workspace-id"

		stagingFiles := lo.RepeatBy(100, func(index int) *model.StagingFile {
			return &model.StagingFile{
				ID: int64(index),
			}
		})

		ctx := context.Background()

		testsCases := []struct {
			name                string
			warehouseType       string
			warehouseSchema     model.Schema
			mockSchemas         []model.Schema
			mockErr             error
			expectedSchema      model.Schema
			wantError           error
			idResolutionEnabled bool
		}{
			{
				name:          "error fetching staging schema",
				warehouseType: whutils.RS,
				mockSchemas:   []model.Schema{},
				mockErr:       errors.New("test error"),
				wantError:     errors.New("getting staging files schema: test error"),
			},

			{
				name:          "discards schema for bigquery",
				warehouseType: whutils.BQ,
				mockSchemas:   []model.Schema{},
				expectedSchema: model.Schema{
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"loaded_at":    "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "discards schema for all destinations",
				warehouseType: whutils.RS,
				mockSchemas:   []model.Schema{},
				expectedSchema: model.Schema{
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "users and identifies should have similar schema except for id and user_id",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"identifies": model.TableSchema{
							"id":               "string",
							"user_id":          "int",
							"anonymous_id":     "string",
							"received_at":      "datetime",
							"sent_at":          "datetime",
							"timestamp":        "datetime",
							"source_id":        "string",
							"destination_id":   "string",
							"source_type":      "string",
							"destination_type": "string",
						},
						"users": model.TableSchema{
							"id":               "string",
							"anonymous_id":     "string",
							"received_at":      "datetime",
							"sent_at":          "datetime",
							"timestamp":        "datetime",
							"source_id":        "string",
							"destination_id":   "string",
							"source_type":      "string",
							"destination_type": "string",
						},
					},
				},
				expectedSchema: model.Schema{
					"identifies": model.TableSchema{
						"id":               "string",
						"user_id":          "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
					"users": model.TableSchema{
						"id":               "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "users have extra properties as compared to identifies",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"identifies": model.TableSchema{
							"id":               "string",
							"user_id":          "int",
							"anonymous_id":     "string",
							"received_at":      "datetime",
							"sent_at":          "datetime",
							"timestamp":        "datetime",
							"source_id":        "string",
							"destination_id":   "string",
							"source_type":      "string",
							"destination_type": "string",
						},
						"users": model.TableSchema{
							"id":               "string",
							"anonymous_id":     "string",
							"received_at":      "datetime",
							"sent_at":          "datetime",
							"timestamp":        "datetime",
							"source_id":        "string",
							"destination_id":   "string",
							"source_type":      "string",
							"destination_type": "string",
							"extra_property_1": "string",
							"extra_property_2": "string",
						},
					},
				},
				expectedSchema: model.Schema{
					"identifies": model.TableSchema{
						"id":               "string",
						"user_id":          "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
					"users": model.TableSchema{
						"id":               "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
						"extra_property_1": "string",
						"extra_property_2": "string",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "users without identifies",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"users": model.TableSchema{
							"id":               "string",
							"anonymous_id":     "string",
							"received_at":      "datetime",
							"sent_at":          "datetime",
							"timestamp":        "datetime",
							"source_id":        "string",
							"destination_id":   "string",
							"source_type":      "string",
							"destination_type": "string",
						},
					},
				},
				expectedSchema: model.Schema{
					"users": model.TableSchema{
						"id":               "string",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "unknown table in warehouse schema",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				warehouseSchema: model.Schema{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "unknown properties in warehouse schema",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				warehouseSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_warehouse_int":       "int",
						"test_warehouse_str":       "string",
						"test_warehouse_bool":      "boolean",
						"test_warehouse_float":     "float",
						"test_warehouse_timestamp": "timestamp",
						"test_warehouse_date":      "date",
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "single staging schema with empty warehouse schema",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "single staging schema with warehouse schema and text data type override",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_text":      "text",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				warehouseSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "text",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:                "id resolution without merge schema",
				warehouseType:       whutils.BQ,
				idResolutionEnabled: true,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"loaded_at":    "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:                "id resolution with merge schema",
				warehouseType:       whutils.BQ,
				idResolutionEnabled: true,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
						"rudder_identity_merge_rules": model.TableSchema{},
						"rudder_identity_mappings":    model.TableSchema{},
					},
				},
				expectedSchema: model.Schema{
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"loaded_at":    "datetime",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
					"rudder_identity_mappings": model.TableSchema{
						"merge_property_type":  "string",
						"merge_property_value": "string",
						"rudder_id":            "string",
						"updated_at":           "datetime",
					},
					"rudder_identity_merge_rules": model.TableSchema{
						"merge_property_1_type":  "string",
						"merge_property_1_value": "string",
						"merge_property_2_type":  "string",
						"merge_property_2_value": "string",
					},
					"test-table": model.TableSchema{
						"test_bool":      "boolean",
						"test_date":      "date",
						"test_float":     "float",
						"test_int":       "int",
						"test_str":       "string",
						"test_timestamp": "timestamp",
					},
				},
			},
			{
				name:          "multiple staging schemas with empty warehouse schema",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table-1": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table-2": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				expectedSchema: model.Schema{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"test-table-2": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "multiple staging schemas with empty warehouse schema and text datatype",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table-1": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_text":      "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table-1": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_text":      "text",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table-1": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_text":      "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table-2": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				expectedSchema: model.Schema{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "text",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"test-table-2": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "multiple staging schemas with warehouse schema and text datatype",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table-1": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_text":      "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table-1": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_text":      "text",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table-1": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_text":      "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table-2": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				warehouseSchema: model.Schema{
					"test-table-1": model.TableSchema{
						"test_int":  "warehouse_int",
						"test_str":  "warehouse_string",
						"test_text": "warehouse_text",
					},
					"test-table-2": model.TableSchema{
						"test_float":     "warehouse_float",
						"test_timestamp": "warehouse_timestamp",
						"test_date":      "warehouse_date",
					},
				},
				expectedSchema: model.Schema{
					"test-table-1": model.TableSchema{
						"test_int":       "warehouse_int",
						"test_str":       "warehouse_string",
						"test_text":      "warehouse_text",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"test-table-2": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "warehouse_float",
						"test_timestamp": "warehouse_timestamp",
						"test_date":      "warehouse_date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "multiple schemas with same table and empty warehouse schema",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":  "int",
							"test_str":  "string",
							"test_bool": "boolean",
						},
					},
					{
						"test-table": model.TableSchema{
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "multiple schemas with same table and warehouse schema",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":  "int",
							"test_str":  "string",
							"test_bool": "boolean",
						},
					},
					{
						"test-table": model.TableSchema{
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
				},
				warehouseSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":   "warehouse_int",
						"test_float": "warehouse_float",
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "warehouse_int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "warehouse_float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "multiple schemas with preference to first schema and empty warehouse schema",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table": model.TableSchema{
							"test_int":       "new_int",
							"test_str":       "new_string",
							"test_bool":      "new_boolean",
							"test_float":     "new_float",
							"test_timestamp": "new_timestamp",
							"test_date":      "new_date",
						},
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "multiple schemas with preference to warehouse schema",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"test-table": model.TableSchema{
							"test_int":       "int",
							"test_str":       "string",
							"test_bool":      "boolean",
							"test_float":     "float",
							"test_timestamp": "timestamp",
							"test_date":      "date",
						},
					},
					{
						"test-table": model.TableSchema{
							"test_int":       "new_int",
							"test_str":       "new_string",
							"test_bool":      "new_boolean",
							"test_float":     "new_float",
							"test_timestamp": "new_timestamp",
							"test_date":      "new_date",
						},
					},
				},
				warehouseSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "warehouse_int",
						"test_str":       "warehouse_string",
						"test_bool":      "warehouse_boolean",
						"test_float":     "warehouse_float",
						"test_timestamp": "warehouse_timestamp",
						"test_date":      "warehouse_date",
					},
				},
				expectedSchema: model.Schema{
					"test-table": model.TableSchema{
						"test_int":       "warehouse_int",
						"test_str":       "warehouse_string",
						"test_bool":      "warehouse_boolean",
						"test_float":     "warehouse_float",
						"test_timestamp": "warehouse_timestamp",
						"test_date":      "warehouse_date",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
			{
				name:          "warehouse users have extra properties as compared to identifies",
				warehouseType: whutils.RS,
				mockSchemas: []model.Schema{
					{
						"identifies": model.TableSchema{
							"id":               "string",
							"user_id":          "int",
							"anonymous_id":     "string",
							"received_at":      "datetime",
							"sent_at":          "datetime",
							"timestamp":        "datetime",
							"source_id":        "string",
							"destination_id":   "string",
							"source_type":      "string",
							"destination_type": "string",
						},
						"users": model.TableSchema{
							"id":               "string",
							"anonymous_id":     "string",
							"received_at":      "datetime",
							"sent_at":          "datetime",
							"timestamp":        "datetime",
							"source_id":        "string",
							"destination_id":   "string",
							"source_type":      "string",
							"destination_type": "string",
						},
					},
				},
				warehouseSchema: model.Schema{
					"identifies": model.TableSchema{
						"id":               "string",
						"user_id":          "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
					"users": model.TableSchema{
						"id":               "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
						"extra_property_1": "string",
						"extra_property_2": "string",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
				expectedSchema: model.Schema{
					"identifies": model.TableSchema{
						"id":               "string",
						"user_id":          "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
						"extra_property_1": "string",
						"extra_property_2": "string",
					},
					"users": model.TableSchema{
						"id":               "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
						"extra_property_1": "string",
						"extra_property_2": "string",
					},
					"rudder_discards": model.TableSchema{
						"column_name":  "string",
						"column_value": "string",
						"received_at":  "datetime",
						"row_id":       "string",
						"table_name":   "string",
						"uuid_ts":      "datetime",
						"reason":       "string",
					},
				},
			},
		}
		for _, tc := range testsCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				warehouse := model.Warehouse{
					Source: backendconfig.SourceT{
						ID: sourceID,
					},
					Destination: backendconfig.DestinationT{
						ID: destinationID,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: destType,
						},
					},
					WorkspaceID: workspaceID,
					Namespace:   namespace,
					Type:        tc.warehouseType,
				}
				conf := config.New()
				conf.Set("Warehouse.enableIDResolution", tc.idResolutionEnabled)
				sch, err := New(context.Background(), warehouse, conf, logger.NOP, stats.NOP, &mockFetchSchemaRepo{}, &mockSchemaRepo{
					schemaMap: map[string]model.WHSchema{
						"test_destination_id_test_namespace": {
							Schema:    tc.warehouseSchema,
							ExpiresAt: time.Now().Add(time.Second * 10),
						},
					},
				}, &mockStagingFileRepo{
					schemas: tc.mockSchemas,
					err:     tc.mockErr,
				})
				require.NoError(t, err)
				uploadSchema, err := sch.ConsolidateStagingFilesSchema(ctx, stagingFiles)
				if tc.wantError == nil {
					require.NoError(t, err)
				} else {
					require.Error(t, err, fmt.Sprintf("got error %v, want error %v", err, tc.wantError))
				}
				require.Equal(t, tc.expectedSchema, uploadSchema)
			})
		}
	})

	t.Run("ExpiresAt updated only on warehouse fetch", func(t *testing.T) {
		mockRepo := &mockSchemaRepo{
			schemaMap: map[string]model.WHSchema{
				"dest_id_namespace": {
					SourceID:      "source_id",
					Namespace:     "namespace",
					DestinationID: "dest_id",
					Schema:        model.Schema{"table": {"col": "string"}},
					ExpiresAt:     timeutil.Now().Add(-10 * time.Minute),
				},
			},
		}
		wh := model.Warehouse{
			Destination: backendconfig.DestinationT{ID: "dest_id"},
			Namespace:   "namespace",
			Source:      backendconfig.SourceT{ID: "source_id"},
		}

		sch := newSchema(t, wh, mockRepo)
		expiresAt := mockRepo.schemaMap["dest_id_namespace"].ExpiresAt
		// ExpiresAt is updated since the schema in DB was expired so it was fetched from warehouse
		require.Greater(t, expiresAt, timeutil.Now())

		err := sch.UpdateSchema(ctx, model.Schema{"table": {"col": "int"}})
		require.NoError(t, err)
		// No change to expiresAt
		require.Equal(t, expiresAt, mockRepo.schemaMap["dest_id_namespace"].ExpiresAt)

		err = sch.UpdateTableSchema(ctx, "table", model.TableSchema{"col": "string"})
		require.NoError(t, err)
		// No change to expiresAt
		require.Equal(t, expiresAt, mockRepo.schemaMap["dest_id_namespace"].ExpiresAt)
	})

	t.Run("ConcurrentUpdateTableSchema", func(t *testing.T) {
		mockRepo := &mockSchemaRepo{
			schemaMap: make(map[string]model.WHSchema),
		}
		sch := newSchema(t, warehouse, mockRepo)
		const numGoroutines = 10

		// Channel to collect errors from goroutines
		errCh := make(chan error, numGoroutines)

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Launch multiple goroutines to update different tables concurrently
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()

				tableName := fmt.Sprintf("concurrent_table_%d", id)
				tableSchema := model.TableSchema{
					fmt.Sprintf("column_%d", id): "string",
				}

				err := sch.UpdateTableSchema(ctx, tableName, tableSchema)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d: %w", id, err)
					return
				}
			}(i)
		}

		// Wait for all goroutines to complete
		wg.Wait()
		close(errCh)

		// Check for any errors
		for err := range errCh {
			t.Fatalf("Concurrent update error: %v", err)
		}

		for i := 0; i < numGoroutines; i++ {
			tableName := fmt.Sprintf("concurrent_table_%d", i)
			table := sch.GetTableSchema(ctx, tableName)

			// Each table should have exactly 1 column
			require.Equal(t, 1, len(table), "Table %s should have 1 column", tableName)

			// Verify the column exists
			columnName := fmt.Sprintf("column_%d", i)
			require.Equal(t, "string", table[columnName], "Column %s should exist in table %s", columnName, tableName)
		}
	})

	t.Run("ConcurrentReadAndWrite", func(t *testing.T) {
		mockRepo := &mockSchemaRepo{
			schemaMap: make(map[string]model.WHSchema),
		}
		sch := newSchema(t, warehouse, mockRepo)
		iterations := 10000

		// Channel to collect errors from goroutines
		errCh := make(chan error, iterations)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			for i := 0; i < iterations; i++ {
				tableName := fmt.Sprintf("concurrent_table_%d", i)
				tableSchema := model.TableSchema{
					fmt.Sprintf("column_%d", i): "string",
				}

				err := sch.UpdateTableSchema(ctx, tableName, tableSchema)
				if err != nil {
					errCh <- fmt.Errorf("write goroutine %d: %w", i, err)
					continue
				}
			}
			wg.Done()
		}()

		go func() {
			for i := 0; i < iterations; i++ {
				tableName := fmt.Sprintf("concurrent_table_%d", i)
				_ = sch.GetTableSchema(ctx, tableName)
			}
			wg.Done()
		}()

		// Wait for all goroutines to complete
		wg.Wait()
		close(errCh)

		// Check for any errors
		for err := range errCh {
			t.Fatalf("Concurrent operation error: %v", err)
		}
	})

	t.Run("SchemaOperationsAcrossConnections", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()
		schemaRepo := repo.NewWHSchemas(db, config.New())

		// Create initial schema for connection 1
		warehouse1 := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "s1",
			},
			Destination: backendconfig.DestinationT{
				ID: "d1",
			},
			Namespace: "n1",
		}
		sch1, err := New(ctx, warehouse1, config.New(), logger.NOP, stats.NOP, &mockFetchSchemaRepo{}, schemaRepo, nil)
		require.NoError(t, err)

		// Create and save initial schema
		initialSchema := model.Schema{
			"table1": {
				"column1": "string",
				"column2": "int",
			},
		}
		err = sch1.UpdateSchema(ctx, initialSchema)
		require.NoError(t, err)

		// Verify initial schema was saved
		require.False(t, sch1.IsSchemaEmpty(ctx))
		require.Equal(t, initialSchema["table1"], sch1.GetTableSchema(ctx, "table1"))

		warehouse2 := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "s2",
			},
			Destination: warehouse1.Destination,
			Namespace:   warehouse1.Namespace,
		}
		sch2, err := New(ctx, warehouse2, config.New(), logger.NOP, stats.NOP, &mockFetchSchemaRepo{}, schemaRepo, nil)
		require.NoError(t, err)

		// Verify schema is same as connection 1
		require.False(t, sch2.IsSchemaEmpty(ctx))
		require.Equal(t, initialSchema["table1"], sch1.GetTableSchema(ctx, "table1"))

		// Save new schema for connection 2
		table2Schema := model.TableSchema{
			"column3": "string",
			"column4": "int",
		}
		err = sch2.UpdateTableSchema(ctx, "table2", table2Schema)
		require.NoError(t, err)

		// Verify new schema was saved
		require.False(t, sch2.IsSchemaEmpty(ctx))
		require.Equal(t, table2Schema, sch2.GetTableSchema(ctx, "table2"))

		// Verify changes are reflected in connection 1
		sch1_new, err := New(ctx, warehouse1, config.New(), logger.NOP, stats.NOP, &mockFetchSchemaRepo{}, schemaRepo, nil)
		require.NoError(t, err)
		require.False(t, sch1_new.IsSchemaEmpty(ctx))
		require.Equal(t, initialSchema["table1"], sch1_new.GetTableSchema(ctx, "table1"))
		require.Equal(t, table2Schema, sch1_new.GetTableSchema(ctx, "table2"))
	})

	t.Run("schema initialization", func(t *testing.T) {
		t.Run("DB returns no schema, warehouse fetch succeeds", func(t *testing.T) {
			schemaRepo := &mockSchemaRepo{schemaMap: make(map[string]model.WHSchema)}
			sh := newSchema(t, warehouse, schemaRepo)
			require.False(t, sh.IsSchemaEmpty(context.Background()))
			tbl := sh.GetTableSchema(context.Background(), "table1")
			require.Equal(t, model.TableSchema{"column1": "string", "column2": "int"}, tbl)
		})

		t.Run("DB returns schema (even empty), no fetching from warehouse", func(t *testing.T) {
			schemaRepo := &mockSchemaRepo{schemaMap: map[string]model.WHSchema{
				"dest_id_namespace": {Schema: model.Schema{}, ExpiresAt: timeutil.Now().Add(10 * time.Minute)},
			}}
			sh := newSchema(t, warehouse, schemaRepo)
			require.True(t, sh.IsSchemaEmpty(context.Background()))
		})

		t.Run("DB returns no schema, warehouse fetch fails", func(t *testing.T) {
			schemaRepo := &mockSchemaRepo{schemaMap: make(map[string]model.WHSchema)}
			fetchSchemaRepo := &mockFetchSchemaRepo{err: errors.New("warehouse fetch error")}
			_, err := New(context.Background(), warehouse, config.New(), logger.NOP, stats.NOP, fetchSchemaRepo, schemaRepo, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "warehouse fetch error")
		})
	})
}

func setupDB(t testing.TB) *sqlmiddleware.DB {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	require.NoError(t, (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse"))

	return sqlmiddleware.New(pgResource.DB)
}
