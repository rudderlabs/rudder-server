package schema

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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

type mockFetchSchemaRepo struct {
	schemaInWarehouse             model.Schema
	unrecognizedSchemaInWarehouse model.Schema
	err                           error
}

func (m *mockFetchSchemaRepo) FetchSchema(context.Context) (model.Schema, error) {
	if m.err != nil {
		return nil, m.err
	}

	return m.schemaInWarehouse, nil
}

func TestSchema_FetchSchemaFromWarehouse(t *testing.T) {
	sourceID := "test_source_id"
	destinationID := "test_destination_id"
	namespace := "test_namespace"
	destType := warehouseutils.RS
	workspaceID := "test-workspace-id"
	tableName := "test-table"
	updatedTable := "updated_test_table"
	updatedSchema := model.TableSchema{
		"updated_test_int":       "int",
		"updated_test_str":       "string",
		"updated_test_bool":      "boolean",
		"updated_test_float":     "float",
		"updated_test_timestamp": "timestamp",
		"updated_test_date":      "date",
		"updated_test_datetime":  "datetime",
	}

	testCases := []struct {
		name           string
		mockSchema     model.Schema
		mockErr        error
		expectedSchema model.Schema
		wantError      error
	}{
		{
			name:           "no schema in warehouse",
			mockSchema:     model.Schema{},
			expectedSchema: model.Schema{},
		},
		{
			name:       "error in fetching schema from warehouse",
			mockSchema: model.Schema{},
			mockErr:    errors.New("test error"),
			wantError:  errors.New("fetching schema from warehouse: test error"),
		},
		{
			name: "no deprecated columns",
			mockSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
					"test_datetime":  "datetime",
				},
			},
			expectedSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
					"test_datetime":  "datetime",
				},
			},
		},
		{
			name: "invalid deprecated column format",
			mockSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":                 "int",
					"test_str":                 "string",
					"test_bool":                "boolean",
					"test_float":               "float",
					"test_timestamp":           "timestamp",
					"test_date":                "date",
					"test_datetime":            "datetime",
					"test-deprecated-column":   "int",
					"test-deprecated-column-1": "string",
					"test-deprecated-column-2": "boolean",
				},
			},
			expectedSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":                 "int",
					"test_str":                 "string",
					"test_bool":                "boolean",
					"test_float":               "float",
					"test_timestamp":           "timestamp",
					"test_date":                "date",
					"test_datetime":            "datetime",
					"test-deprecated-column":   "int",
					"test-deprecated-column-1": "string",
					"test-deprecated-column-2": "boolean",
				},
			},
		},
		{
			name: "valid deprecated column format",
			mockSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":                 "int",
					"test_str":                 "string",
					"test_bool":                "boolean",
					"test_float":               "float",
					"test_timestamp":           "timestamp",
					"test_date":                "date",
					"test_datetime":            "datetime",
					"test-deprecated-column":   "int",
					"test-deprecated-column-1": "string",
					"test-deprecated-column-2": "boolean",
					"test-deprecated-546a4f59-c303-474e-b2c7-cf37361b5c2f": "int",
					"test-deprecated-c60bf1e9-7cbd-42d4-8a7d-af01f5ff7d8b": "string",
					"test-deprecated-bc3bbc6d-42c9-4d2c-b0e6-bf5820914b09": "boolean",
				},
			},
			expectedSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":                 "int",
					"test_str":                 "string",
					"test_bool":                "boolean",
					"test_float":               "float",
					"test_timestamp":           "timestamp",
					"test_date":                "date",
					"test_datetime":            "datetime",
					"test-deprecated-column":   "int",
					"test-deprecated-column-1": "string",
					"test-deprecated-column-2": "boolean",
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockRepo := mockFetchSchemaRepo{
				schemaInWarehouse:             tc.mockSchema,
				unrecognizedSchemaInWarehouse: tc.mockSchema,
				err:                           tc.mockErr,
			}

			s := &Schema{
				warehouse: model.Warehouse{
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
				},
				log: logger.NOP,
			}

			ctx := context.Background()

			require.Empty(t, s.GetTableSchemaInWarehouse(tableName))
			require.True(t, s.IsSchemaEmpty())

			_, err := s.FetchSchemaFromWarehouse(ctx, &mockRepo)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err, fmt.Sprintf("got error %v, want error %v", err, tc.wantError))
			}
			require.Equal(t, tc.expectedSchema[tableName], s.GetTableSchemaInWarehouse(tableName))
			require.Equal(t, len(tc.expectedSchema[tableName]), s.GetColumnsCountInSchema(tableName))
			if len(tc.expectedSchema) > 0 {
				require.False(t, s.IsSchemaEmpty())
			} else {
				require.True(t, s.IsSchemaEmpty())
			}
			s.UpdateWarehouseTableSchema(updatedTable, updatedSchema)
			require.Equal(t, updatedSchema, s.GetTableSchemaInWarehouse(updatedTable))
			require.False(t, s.IsSchemaEmpty())
		})
	}
}

func TestSchema_TableSchemaDiff(t *testing.T) {
	testCases := []struct {
		name              string
		tableName         string
		currentSchema     model.Schema
		uploadTableSchema model.TableSchema
		expected          warehouseutils.TableSchemaDiff
	}{
		{
			name:          "empty current and upload schema",
			tableName:     "test-table",
			currentSchema: model.Schema{},
			expected: warehouseutils.TableSchemaDiff{
				ColumnMap:        model.TableSchema{},
				UpdatedSchema:    model.TableSchema{},
				AlteredColumnMap: model.TableSchema{},
			},
		},
		{
			name:          "empty current schema",
			tableName:     "test-table",
			currentSchema: model.Schema{},
			uploadTableSchema: model.TableSchema{
				"test-column": "test-value",
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: true,
				ColumnMap: model.TableSchema{
					"test-column": "test-value",
				},
				UpdatedSchema: model.TableSchema{
					"test-column": "test-value",
				},
				AlteredColumnMap: model.TableSchema{},
			},
		},
		{
			name:      "override existing column",
			tableName: "test-table",
			currentSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-1",
				},
			},
			uploadTableSchema: model.TableSchema{
				"test-column": "test-value-2",
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           false,
				TableToBeCreated: false,
				ColumnMap:        model.TableSchema{},
				UpdatedSchema: model.TableSchema{
					"test-column": "test-value-1",
				},
				AlteredColumnMap: model.TableSchema{},
			},
		},
		{
			name:      "union of current and upload schema",
			tableName: "test-table",
			currentSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
				},
			},
			uploadTableSchema: model.TableSchema{
				"test-column": "test-value-2",
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: false,
				ColumnMap: model.TableSchema{
					"test-column": "test-value-2",
				},
				UpdatedSchema: model.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
					"test-column":   "test-value-2",
				},
				AlteredColumnMap: model.TableSchema{},
			},
		},
		{
			name:      "override text column with string column",
			tableName: "test-table",
			currentSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column":   "string",
					"test-column-2": "test-value-2",
				},
			},
			uploadTableSchema: model.TableSchema{
				"test-column": "text",
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: false,
				ColumnMap:        model.TableSchema{},
				UpdatedSchema: model.TableSchema{
					"test-column-2": "test-value-2",
					"test-column":   "text",
				},
				AlteredColumnMap: model.TableSchema{
					"test-column": "text",
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := Schema{}
			diff := s.TableSchemaDiff(tc.tableName, tc.uploadTableSchema)
			require.EqualValues(t, diff, tc.expected)
		})
	}
}

func TestSchema_HasLocalSchemaChanged(t *testing.T) {
	testCases := []struct {
		name              string
		localSchema       model.Schema
		schemaInWarehouse model.Schema
		expected          bool
	}{
		{
			name:              "When both schemas are empty and skipDeepEquals is false",
			localSchema:       model.Schema{},
			schemaInWarehouse: model.Schema{},
			expected:          false,
		},
		{
			name:        "When local schema is empty and skipDeepEquals is false",
			localSchema: model.Schema{},
			schemaInWarehouse: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value",
				},
			},
			expected: true,
		},
		{
			name: "same table, same column, different datatype and skipDeepEquals is false",
			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-1",
				},
			},
			schemaInWarehouse: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-2",
				},
			},
			expected: true,
		},
		{
			name: "same table, different columns and skipDeepEquals is false",
			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
				},
			},
			schemaInWarehouse: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-2",
				},
			},
			expected: true,
		},
		{
			name: "different table and skipDeepEquals is false",
			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "string",
				},
			},
			schemaInWarehouse: model.Schema{
				"test-table-1": model.TableSchema{
					"test-column": "text",
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &Schema{
				warehouse: model.Warehouse{
					Type: warehouseutils.SNOWFLAKE,
				},
			}
			require.Equal(t, tc.expected, s.HasSchemaChanged(tc.localSchema))
		})
	}
}

func TestSchema_ConsolidateStagingFilesUsingLocalSchema(t *testing.T) {
	sourceID := "test_source_id"
	destinationID := "test_destination_id"
	namespace := "test_namespace"
	destType := warehouseutils.RS
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
			warehouseType: warehouseutils.RS,
			mockSchemas:   []model.Schema{},
			mockErr:       errors.New("test error"),
			wantError:     errors.New("getting staging files schema: test error"),
		},

		{
			name:          "discards schema for bigquery",
			warehouseType: warehouseutils.BQ,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType:       warehouseutils.BQ,
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
			warehouseType:       warehouseutils.BQ,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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
			warehouseType: warehouseutils.RS,
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

			mockRepo := &mockStagingFileRepo{
				schemas: tc.mockSchemas,
				err:     tc.mockErr,
			}

			s := &Schema{
				warehouse: model.Warehouse{
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
				},
				log:                              logger.NOP,
				stagingFileRepo:                  mockRepo,
				enableIDResolution:               tc.idResolutionEnabled,
				localSchema:                      tc.warehouseSchema,
				stagingFilesSchemaPaginationSize: 2,
			}

			uploadSchema, err := s.ConsolidateStagingFilesUsingLocalSchema(ctx, stagingFiles)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err, fmt.Sprintf("got error %v, want error %v", err, tc.wantError))
			}
			require.Equal(t, tc.expectedSchema, uploadSchema)
		})
	}
}
