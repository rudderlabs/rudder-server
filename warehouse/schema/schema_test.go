package schema

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func schemaKey(sourceID, destinationID, namespace string) string {
	return fmt.Sprintf("%s_%s_%s", sourceID, destinationID, namespace)
}

type mockSchemaRepo struct {
	err       error
	schemaMap map[string]model.WHSchema
}

func (m *mockSchemaRepo) GetForNamespace(_ context.Context, sourceID, destinationID, namespace string) (model.WHSchema, error) {
	if m.err != nil {
		return model.WHSchema{}, m.err
	}

	key := schemaKey(sourceID, destinationID, namespace)
	return m.schemaMap[key], nil
}

func (m *mockSchemaRepo) Insert(_ context.Context, schema *model.WHSchema) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}

	key := schemaKey(schema.SourceID, schema.DestinationID, schema.Namespace)
	m.schemaMap[key] = *schema
	return 0, nil
}

type mockFetchSchemaRepo struct {
	count int
}

func (m *mockFetchSchemaRepo) FetchSchema(ctx context.Context) (model.Schema, error) {
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

var ttl = 10 * time.Minute

func newSchema(warehouse model.Warehouse, schemaRepo schemaRepo) *schema {
	return &schema{
		warehouse:                        warehouse,
		log:                              logger.NOP,
		ttlInMinutes:                     ttl,
		schemaRepo:                       schemaRepo,
		fetchSchemaRepo:                  &mockFetchSchemaRepo{},
		now:                              timeutil.Now,
		stagingFilesSchemaPaginationSize: 2,
	}
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
	sch := newSchema(warehouse, &mockSchemaRepo{
		schemaMap: make(map[string]model.WHSchema),
	})
	ctx := context.Background()

	t.Run("SyncRemoteSchema", func(t *testing.T) {
		schema, err := sch.getSchema(ctx)
		require.NoError(t, err)
		require.Equal(t, model.Schema{}, schema)
		require.True(t, sch.IsWarehouseSchemaEmpty(ctx))

		_, err = sch.SyncRemoteSchema(ctx, nil, 0)
		require.NoError(t, err)
		schema, err = sch.getSchema(ctx)
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
		sch := newSchema(warehouse, &mockSchemaRepo{
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
		})
		count, err := sch.GetColumnsCountInWarehouseSchema(ctx, "table2")
		require.NoError(t, err)
		require.Equal(t, 1, count)
		_, err = sch.SyncRemoteSchema(ctx, nil, 0)
		require.NoError(t, err)
		count, err = sch.GetColumnsCountInWarehouseSchema(ctx, "table2")
		require.NoError(t, err)
		require.Equal(t, 3, count)

		sch.now = func() time.Time {
			return timeutil.Now().Add(ttl * 2)
		}
		count, err = sch.GetColumnsCountInWarehouseSchema(ctx, "table2")
		require.NoError(t, err)
		require.Equal(t, 4, count)
	})

	t.Run("TableSchemaDiff", func(t *testing.T) {
		err := sch.UpdateWarehouseTableSchema(ctx, "table2", model.TableSchema{
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

	t.Run("ConsolidateStagingFilesUsingLocalSchema", func(t *testing.T) {
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
			tc := tc

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

				sch := newSchema(warehouse, &mockSchemaRepo{
					schemaMap: map[string]model.WHSchema{
						"test_source_id_test_destination_id_test_namespace": {
							Schema: tc.warehouseSchema,
						},
					},
				})
				sch.stagingFileRepo = &mockStagingFileRepo{
					schemas: tc.mockSchemas,
					err:     tc.mockErr,
				}
				sch.enableIDResolution = tc.idResolutionEnabled
				sch.now = func() time.Time {
					return time.Time{}
				}
				uploadSchema, err := sch.ConsolidateStagingFilesUsingLocalSchema(ctx, stagingFiles)
				if tc.wantError == nil {
					require.NoError(t, err)
				} else {
					require.Error(t, err, fmt.Sprintf("got error %v, want error %v", err, tc.wantError))
				}
				require.Equal(t, tc.expectedSchema, uploadSchema)
			})
		}
	})
}
