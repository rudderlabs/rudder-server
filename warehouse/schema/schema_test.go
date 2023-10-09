package schema

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockSchemaRepo struct {
	err       error
	schemaMap map[string]model.WHSchema
}

func (m *mockSchemaRepo) GetForNamespace(_ context.Context, sourceID, destID, namespace string) (model.WHSchema, error) {
	key := fmt.Sprintf("%s_%s_%s", sourceID, destID, namespace)

	return m.schemaMap[key], m.err
}

func (m *mockSchemaRepo) Insert(_ context.Context, schema *model.WHSchema) (int64, error) {
	if schema == nil {
		return 0, m.err
	}
	key := fmt.Sprintf("%s_%s_%s", schema.SourceID, schema.DestinationID, schema.Namespace)

	m.schemaMap[key] = *schema
	return int64(len(m.schemaMap)), m.err
}

type mockFetchSchemaFromWarehouse struct {
	schemaInWarehouse             model.Schema
	unrecognizedSchemaInWarehouse model.Schema
	err                           error
}

func (m *mockFetchSchemaFromWarehouse) FetchSchema(context.Context) (model.Schema, model.Schema, error) {
	return m.schemaInWarehouse, m.unrecognizedSchemaInWarehouse, m.err
}

type mockStagingFileRepo struct {
	schemas []model.Schema
	err     error
}

func (m *mockStagingFileRepo) GetSchemasByIDs(context.Context, []int64) ([]model.Schema, error) {
	return m.schemas, m.err
}

func TestSchema_GetUpdateLocalSchema(t *testing.T) {
	const (
		sourceID      = "test_source"
		destID        = "test_dest"
		namespace     = "test_namespace"
		warehouseType = warehouseutils.RS
		uploadID      = 1
	)

	testCases := []struct {
		name          string
		mockSchema    model.WHSchema
		mockSchemaErr error
		wantSchema    model.Schema
		wantError     error
	}{
		{
			name:          "no schema in db",
			mockSchema:    model.WHSchema{},
			mockSchemaErr: nil,
			wantSchema:    nil,
			wantError:     nil,
		},
		{
			name:          "error in fetching schema from db",
			mockSchema:    model.WHSchema{},
			mockSchemaErr: errors.New("test error"),
			wantSchema:    nil,
			wantError:     errors.New("test error"),
		},
		{
			name: "schema in db",
			mockSchema: model.WHSchema{
				Schema: model.Schema{
					"table1": model.TableSchema{
						"column1": "string",
						"column2": "int",
						"column3": "float",
						"column4": "datetime",
						"column5": "json",
					},
				},
			},
			mockSchemaErr: nil,
			wantSchema: model.Schema{
				"table1": model.TableSchema{
					"column1": "string",
					"column2": "int",
					"column3": "float",
					"column4": "datetime",
					"column5": "json",
				},
			},
			wantError: nil,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockSchemaRepo := &mockSchemaRepo{
				err:       tc.mockSchemaErr,
				schemaMap: map[string]model.WHSchema{},
			}

			sch := Schema{
				warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						ID: sourceID,
					},
					Destination: backendconfig.DestinationT{
						ID: destID,
					},
					Namespace: namespace,
					Type:      warehouseType,
				},
				schemaRepo: mockSchemaRepo,
			}

			ctx := context.Background()

			err := sch.UpdateLocalSchema(ctx, uploadID, tc.mockSchema.Schema)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError.Error())
			}

			require.Equal(t, tc.wantSchema, sch.localSchema)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError.Error())
			}
		})
	}
}

func TestSchema_FetchSchemaFromWarehouse(t *testing.T) {
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
				"test-table": model.TableSchema{
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
				"test-table": model.TableSchema{
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
				"test-table": model.TableSchema{
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
				"test-table": model.TableSchema{
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
				"test-table": model.TableSchema{
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
				"test-table": model.TableSchema{
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

	const (
		sourceID    = "test-source-id"
		destID      = "test-dest-id"
		destType    = "RS"
		workspaceID = "test-workspace-id"
		namespace   = "test-namespace"
	)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fechSchemaRepo := mockFetchSchemaFromWarehouse{
				schemaInWarehouse:             tc.mockSchema,
				unrecognizedSchemaInWarehouse: tc.mockSchema,
				err:                           tc.mockErr,
			}

			sh := &Schema{
				warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						ID: sourceID,
					},
					Destination: backendconfig.DestinationT{
						ID: destID,
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

			err := sh.FetchSchemaFromWarehouse(ctx, &fechSchemaRepo)
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedSchema, sh.schemaInWarehouse)
			require.Equal(t, tc.expectedSchema, sh.unrecognizedSchemaInWarehouse)
		})
	}
}

func TestSchema_GetUploadSchemaDiff(t *testing.T) {
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

			sch := Schema{
				schemaInWarehouse: tc.currentSchema,
			}
			diff := sch.TableSchemaDiff(tc.tableName, tc.uploadTableSchema)
			require.EqualValues(t, diff, tc.expected)
		})
	}
}

func TestSchema_HasLocalSchemaChanged(t *testing.T) {
	testCases := []struct {
		name              string
		localSchema       model.Schema
		schemaInWarehouse model.Schema
		skipDeepEquals    bool
		expected          bool
	}{
		{
			name:              "When both schemas are empty and skipDeepEquals is true",
			localSchema:       model.Schema{},
			schemaInWarehouse: model.Schema{},
			skipDeepEquals:    true,
			expected:          false,
		},
		{
			name:        "When local schema is empty and skipDeepEquals is true",
			localSchema: model.Schema{},
			schemaInWarehouse: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value",
				},
			},
			skipDeepEquals: true,
			expected:       false,
		},
		{
			name: "same table, same column, different datatype and skipDeepEquals is true",
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
			skipDeepEquals: true,
			expected:       true,
		},
		{
			name: "same table, different columns and skipDeepEquals is true",
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
			skipDeepEquals: true,
			expected:       true,
		},
		{
			name: "different table and skipDeepEquals is true",
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
			skipDeepEquals: true,
			expected:       true,
		},
		{
			name:              "When both schemas are empty and skipDeepEquals is false",
			localSchema:       model.Schema{},
			schemaInWarehouse: model.Schema{},
			skipDeepEquals:    false,
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
			skipDeepEquals: false,
			expected:       true,
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
			skipDeepEquals: false,
			expected:       true,
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
			skipDeepEquals: false,
			expected:       true,
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
			skipDeepEquals: false,
			expected:       true,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sch := &Schema{
				warehouse: model.Warehouse{
					Type: warehouseutils.SNOWFLAKE,
				},
				skipDeepEqualSchemas: tc.skipDeepEquals,
				schemaInWarehouse:    tc.schemaInWarehouse,
			}
			require.Equal(t, tc.expected, sch.hasSchemaChanged(tc.localSchema))
		})
	}
}

func TestSchema_ConsolidateStagingFilesUsingLocalSchema(t *testing.T) {
	warehouseutils.Init()

	const (
		sourceID    = "test-source-id"
		destID      = "test-dest-id"
		destType    = "BQ"
		workspaceID = "test-workspace-id"
		namespace   = "test-namespace"
	)

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
				},
			},
		},
	}
	for _, tc := range testsCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sh := &Schema{
				warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						ID: sourceID,
					},
					Destination: backendconfig.DestinationT{
						ID: destID,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: destType,
						},
					},
					WorkspaceID: workspaceID,
					Namespace:   namespace,
					Type:        tc.warehouseType,
				},
				log: logger.NOP,
				stagingFileRepo: &mockStagingFileRepo{
					schemas: tc.mockSchemas,
					err:     tc.mockErr,
				},
				enableIDResolution:               tc.idResolutionEnabled,
				localSchema:                      tc.warehouseSchema,
				stagingFilesSchemaPaginationSize: 2,
			}

			uploadSchema, err := sh.ConsolidateStagingFilesUsingLocalSchema(ctx, stagingFiles)
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedSchema, uploadSchema)
		})
	}
}

func TestSchema_SyncRemoteSchema(t *testing.T) {
	sourceID := "test_source_id"
	destinationID := "test_destination_id"
	namespace := "test_namespace"
	destType := warehouseutils.RS
	workspaceID := "test-workspace-id"
	uploadID := int64(1)
	tableName := "test_table_name"

	schemaKey := fmt.Sprintf("%s_%s_%s", sourceID, destinationID, namespace)

	t.Run("should return error if unable to fetch local schema", func(t *testing.T) {
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
				Type:        destType,
			},
			schemaRepo: &mockSchemaRepo{
				err:       errors.New("test error"),
				schemaMap: map[string]model.WHSchema{},
			},
			log: logger.NOP,
		}

		ctx := context.Background()

		schemaChanged, err := s.SyncRemoteSchema(ctx, &mockFetchSchemaFromWarehouse{}, uploadID)
		require.Error(t, err, "got error %v, want error %v", err, "fetching schema from local: test error")
		require.False(t, schemaChanged)
	})
	t.Run("should return error if unable to fetch remote schema", func(t *testing.T) {
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
				Type:        destType,
			},
			schemaRepo: &mockSchemaRepo{
				err:       nil,
				schemaMap: map[string]model.WHSchema{},
			},
			log: logger.NOP,
		}

		mockFetchSchemaRepo := &mockFetchSchemaFromWarehouse{
			err: errors.New("test error"),
		}

		ctx := context.Background()

		schemaChanged, err := s.SyncRemoteSchema(ctx, mockFetchSchemaRepo, uploadID)
		require.Error(t, err, "got error %v, want error %v", err, "fetching schema from warehouse: test error")
		require.False(t, schemaChanged)
	})
	t.Run("schema changed", func(t *testing.T) {
		testSchema := model.Schema{
			tableName: model.TableSchema{
				"test_int":       "int",
				"test_str":       "string",
				"test_bool":      "boolean",
				"test_float":     "float",
				"test_timestamp": "timestamp",
				"test_date":      "date",
				"test_datetime":  "datetime",
			},
		}
		schemaInWarehouse := model.Schema{
			tableName: model.TableSchema{
				"warehouse_test_int":       "int",
				"warehouse_test_str":       "string",
				"warehouse_test_bool":      "boolean",
				"warehouse_test_float":     "float",
				"warehouse_test_timestamp": "timestamp",
				"warehouse_test_date":      "date",
				"warehouse_test_datetime":  "datetime",
			},
		}

		mockSchemaRepo := &mockSchemaRepo{
			err: nil,
			schemaMap: map[string]model.WHSchema{
				schemaKey: {
					Schema: testSchema,
				},
			},
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
				Type:        destType,
			},
			schemaRepo: mockSchemaRepo,
			log:        logger.NOP,
		}

		mockFetchSchemaRepo := &mockFetchSchemaFromWarehouse{
			err:                           nil,
			schemaInWarehouse:             schemaInWarehouse,
			unrecognizedSchemaInWarehouse: schemaInWarehouse,
		}

		ctx := context.Background()

		schemaChanged, err := s.SyncRemoteSchema(ctx, mockFetchSchemaRepo, uploadID)
		require.NoError(t, err)
		require.True(t, schemaChanged)
		require.Equal(t, schemaInWarehouse, s.localSchema)
		require.Equal(t, schemaInWarehouse, mockSchemaRepo.schemaMap[schemaKey].Schema)
		require.Equal(t, schemaInWarehouse, s.schemaInWarehouse)
		require.Equal(t, schemaInWarehouse, s.unrecognizedSchemaInWarehouse)
	})
	t.Run("schema not changed", func(t *testing.T) {
		testSchema := model.Schema{
			tableName: model.TableSchema{
				"test_int":       "int",
				"test_str":       "string",
				"test_bool":      "boolean",
				"test_float":     "float",
				"test_timestamp": "timestamp",
				"test_date":      "date",
				"test_datetime":  "datetime",
			},
		}

		mockSchemaRepo := &mockSchemaRepo{
			err: nil,
			schemaMap: map[string]model.WHSchema{
				schemaKey: {
					Schema: testSchema,
				},
			},
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
				Type:        destType,
			},
			schemaRepo: mockSchemaRepo,
			log:        logger.NOP,
		}

		mockFetchSchemaRepo := &mockFetchSchemaFromWarehouse{
			err:                           nil,
			schemaInWarehouse:             testSchema,
			unrecognizedSchemaInWarehouse: testSchema,
		}

		ctx := context.Background()

		schemaChanged, err := s.SyncRemoteSchema(ctx, mockFetchSchemaRepo, uploadID)
		require.NoError(t, err)
		require.False(t, schemaChanged)
		require.Equal(t, testSchema, s.localSchema)
		require.Equal(t, testSchema, s.schemaInWarehouse)
		require.Equal(t, testSchema, s.unrecognizedSchemaInWarehouse)
	})
}
