package schema_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/schema"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestHandleSchemaChange(t *testing.T) {
	inputs := []struct {
		name             string
		existingDatatype string
		currentDataType  string
		value            any

		newColumnVal any
		convError    error
	}{
		{
			name:             "should send int values if existing datatype is int, new datatype is float",
			existingDatatype: "int",
			currentDataType:  "float",
			value:            1.501,
			newColumnVal:     1,
		},
		{
			name:             "should send float values if existing datatype is float, new datatype is int",
			existingDatatype: "float",
			currentDataType:  "int",
			value:            1,
			newColumnVal:     1.0,
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is boolean",
			existingDatatype: "string",
			currentDataType:  "boolean",
			value:            false,
			newColumnVal:     "false",
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is int",
			existingDatatype: "string",
			currentDataType:  "int",
			value:            1,
			newColumnVal:     "1",
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is float",
			existingDatatype: "string",
			currentDataType:  "float",
			value:            1.501,
			newColumnVal:     "1.501",
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is datetime",
			existingDatatype: "string",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			newColumnVal:     "2022-05-05T00:00:00.000Z",
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is string",
			existingDatatype: "string",
			currentDataType:  "json",
			value:            `{"json":true}`,
			newColumnVal:     `{"json":true}`,
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is boolean",
			existingDatatype: "json",
			currentDataType:  "boolean",
			value:            false,
			newColumnVal:     "false",
		},
		{
			name:             "should send json string values if existing datatype is jso, new datatype is int",
			existingDatatype: "json",
			currentDataType:  "int",
			value:            1,
			newColumnVal:     "1",
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is float",
			existingDatatype: "json",
			currentDataType:  "float",
			value:            1.501,
			newColumnVal:     "1.501",
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is json",
			existingDatatype: "json",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			newColumnVal:     `"2022-05-05T00:00:00.000Z"`,
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is string",
			existingDatatype: "json",
			currentDataType:  "string",
			value:            "string value",
			newColumnVal:     `"string value"`,
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is array",
			existingDatatype: "json",
			currentDataType:  "array",
			value:            []any{false, 1, "string value"},
			newColumnVal:     []any{false, 1, "string value"},
		},
		{
			name:             "existing datatype is boolean, new datatype is int",
			existingDatatype: "boolean",
			currentDataType:  "int",
			value:            1,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is float",
			existingDatatype: "boolean",
			currentDataType:  "float",
			value:            1.501,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is string",
			existingDatatype: "boolean",
			currentDataType:  "string",
			value:            "string value",
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is datetime",
			existingDatatype: "boolean",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is json",
			existingDatatype: "boolean",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is boolean",
			existingDatatype: "int",
			currentDataType:  "boolean",
			value:            false,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is string",
			existingDatatype: "int",
			currentDataType:  "string",
			value:            "string value",
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is datetime",
			existingDatatype: "int",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is json",
			existingDatatype: "int",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is float",
			existingDatatype: "int",
			currentDataType:  "float",
			value:            1,
			convError:        schema.ErrIncompatibleSchemaConversion,
		},
		{
			name:             "existing datatype is float, new datatype is boolean",
			existingDatatype: "float",
			currentDataType:  "boolean",
			value:            false,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is int",
			existingDatatype: "float",
			currentDataType:  "int",
			value:            1.0,
			convError:        schema.ErrIncompatibleSchemaConversion,
		},
		{
			name:             "existing datatype is float, new datatype is string",
			existingDatatype: "float",
			currentDataType:  "string",
			value:            "string value",
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is datetime",
			existingDatatype: "float",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is json",
			existingDatatype: "float",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is boolean",
			existingDatatype: "datetime",
			currentDataType:  "boolean",
			value:            false,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is string",
			existingDatatype: "datetime",
			currentDataType:  "string",
			value:            "string value",
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is int",
			existingDatatype: "datetime",
			currentDataType:  "int",
			value:            1,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is float",
			existingDatatype: "datetime",
			currentDataType:  "float",
			value:            1.501,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is json",
			existingDatatype: "datetime",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        schema.ErrSchemaConversionNotSupported,
		},
	}
	for _, ip := range inputs {
		tc := ip

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			newColumnVal, convError := schema.HandleSchemaChange(
				model.SchemaType(tc.existingDatatype),
				model.SchemaType(tc.currentDataType),
				tc.value,
			)
			require.Equal(t, newColumnVal, tc.newColumnVal)
			require.Equal(t, convError, tc.convError)
		})
	}
}

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

func (m *mockFetchSchemaFromWarehouse) FetchSchema(model.Warehouse) (model.Schema, model.Schema, error) {
	return m.schemaInWarehouse, m.unrecognizedSchemaInWarehouse, m.err
}

type mockStagingFileRepo struct {
	schemas []model.Schema
	err     error
}

func (m *mockStagingFileRepo) GetSchemasByIDs(ctx context.Context, ids []int64) ([]model.Schema, error) {
	return m.schemas, m.err
}

func TestHandler_LocalSchema(t *testing.T) {
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
			wantSchema:    model.Schema{},
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

			handler := schema.Handler{
				Warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						ID: sourceID,
					},
					Destination: backendconfig.DestinationT{
						ID: destID,
					},
					Namespace: namespace,
					Type:      warehouseType,
				},
				WhSchemaRepo: mockSchemaRepo,
			}

			err := handler.UpdateLocalSchema(uploadID, tc.mockSchema.Schema)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError.Error())
			}

			localSchema, err := handler.GetLocalSchema()
			require.Equal(t, tc.wantSchema, localSchema)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError.Error())
			}
		})
	}
}

func TestHandler_FetchSchemaFromWarehouse(t *testing.T) {
	warehouse.Init4()

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
			name:           "error in fetching schema from warehouse",
			mockSchema:     model.Schema{},
			mockErr:        errors.New("test error"),
			expectedSchema: model.Schema{},
			wantError:      errors.New("fetching schema from warehouse: test error"),
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

			sh := &schema.Handler{
				Warehouse: model.Warehouse{
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
				Logger: logger.NOP,
			}

			schemaInWarehouse, unrecognizedSchemaInWarehouse, err := sh.FetchSchemaFromWarehouse(&fechSchemaRepo)
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedSchema, schemaInWarehouse)
			require.Equal(t, tc.expectedSchema, unrecognizedSchemaInWarehouse)
		})
	}
}

func TestGetTableSchemaDiff(t *testing.T) {
	testCases := []struct {
		name          string
		tableName     string
		currentSchema model.Schema
		uploadSchema  model.Schema
		expected      warehouseutils.TableSchemaDiff
	}{
		{
			name:          "empty current and upload schema",
			tableName:     "test-table",
			currentSchema: model.Schema{},
			uploadSchema:  model.Schema{},
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
			uploadSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value",
				},
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
			uploadSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-2",
				},
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
			uploadSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-2",
				},
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
			uploadSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "text",
				},
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

			diff := schema.GetTableSchemaDiff(tc.tableName, tc.currentSchema, tc.uploadSchema)
			require.EqualValues(t, diff, tc.expected)
		})
	}
}

func TestHandler_HasSchemaChanged(t *testing.T) {
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

			handler := &schema.Handler{
				Warehouse: model.Warehouse{
					Type: warehouseutils.SNOWFLAKE,
				},
				SkipDeepEqualSchemas: tc.skipDeepEquals,
			}
			hasSchemaChanged := handler.HasSchemaChanged(tc.localSchema, tc.schemaInWarehouse)
			require.Equal(t, tc.expected, hasSchemaChanged)
		})
	}
}

func TestHandler_ConsolidateStagingFilesSchemaUsingWarehouseSchema(t *testing.T) {
	const (
		sourceID    = "test-source-id"
		destID      = "test-dest-id"
		destType    = "RS"
		workspaceID = "test-workspace-id"
		namespace   = "test-namespace"
	)

	stagingFiles := lo.RepeatBy(100, func(index int) *model.StagingFile {
		return &model.StagingFile{
			ID: int64(index),
		}
	})

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
			name:           "error fetching staging schema",
			warehouseType:  warehouseutils.RS,
			mockSchemas:    []model.Schema{},
			mockErr:        errors.New("test error"),
			expectedSchema: model.Schema{},
			wantError:      errors.New("getting staging files schema: test error"),
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
	}
	for _, tc := range testsCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sh := &schema.Handler{
				Warehouse: model.Warehouse{
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
				Logger: logger.NOP,
				StagingRepo: &mockStagingFileRepo{
					schemas: tc.mockSchemas,
					err:     tc.mockErr,
				},
				IDResolutionEnabled: tc.idResolutionEnabled,
				LocalSchema:         tc.warehouseSchema,
			}

			conf := config.New()
			conf.Set("Warehouse.stagingFilesSchemaPaginationSize", "2")

			schema.WithConfig(sh, conf)

			consolidatedSchema, err := sh.ConsolidateStagingFilesSchemaUsingWarehouseSchema(stagingFiles)
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedSchema, consolidatedSchema)
		})
	}
}
