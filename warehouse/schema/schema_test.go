package schema_test

import (
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
	"testing"

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

func TestGetTableSchemaDiff(t *testing.T) {
	testCases := []struct {
		name          string
		tableName     string
		currentSchema warehouseutils.Schema
		uploadSchema  warehouseutils.Schema
		expected      warehouseutils.TableSchemaDiff
	}{
		{
			name:          "empty current and upload schema",
			tableName:     "test-table",
			currentSchema: warehouseutils.Schema{},
			uploadSchema:  warehouseutils.Schema{},
			expected: warehouseutils.TableSchemaDiff{
				ColumnMap:        warehouseutils.TableSchema{},
				UpdatedSchema:    warehouseutils.TableSchema{},
				AlteredColumnMap: warehouseutils.TableSchema{},
			},
		},
		{
			name:          "empty current schema",
			tableName:     "test-table",
			currentSchema: warehouseutils.Schema{},
			uploadSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value",
				},
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: true,
				ColumnMap: warehouseutils.TableSchema{
					"test-column": "test-value",
				},
				UpdatedSchema: warehouseutils.TableSchema{
					"test-column": "test-value",
				},
				AlteredColumnMap: warehouseutils.TableSchema{},
			},
		},
		{
			name:      "override existing column",
			tableName: "test-table",
			currentSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-1",
				},
			},
			uploadSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-2",
				},
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           false,
				TableToBeCreated: false,
				ColumnMap:        warehouseutils.TableSchema{},
				UpdatedSchema: warehouseutils.TableSchema{
					"test-column": "test-value-1",
				},
				AlteredColumnMap: warehouseutils.TableSchema{},
			},
		},
		{
			name:      "union of current and upload schema",
			tableName: "test-table",
			currentSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
				},
			},
			uploadSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-2",
				},
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: false,
				ColumnMap: warehouseutils.TableSchema{
					"test-column": "test-value-2",
				},
				UpdatedSchema: warehouseutils.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
					"test-column":   "test-value-2",
				},
				AlteredColumnMap: warehouseutils.TableSchema{},
			},
		},
		{
			name:      "override text column with string column",
			tableName: "test-table",
			currentSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column":   "string",
					"test-column-2": "test-value-2",
				},
			},
			uploadSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "text",
				},
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: false,
				ColumnMap:        warehouseutils.TableSchema{},
				UpdatedSchema: warehouseutils.TableSchema{
					"test-column-2": "test-value-2",
					"test-column":   "text",
				},
				AlteredColumnMap: warehouseutils.TableSchema{
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

func TestHandler_GetMergeRulesSchema(t *testing.T) {
	testCases := []struct {
		name           string
		destType       string
		expectedSchema warehouseutils.TableSchema
	}{
		{
			name:     "For snowflake",
			destType: warehouseutils.SNOWFLAKE,
			expectedSchema: warehouseutils.TableSchema{
				"MERGE_PROPERTY_1_TYPE":  "string",
				"MERGE_PROPERTY_1_VALUE": "string",
				"MERGE_PROPERTY_2_TYPE":  "string",
				"MERGE_PROPERTY_2_VALUE": "string",
			},
		},
		{
			name:     "For redshift",
			destType: warehouseutils.RS,
			expectedSchema: warehouseutils.TableSchema{
				"merge_property_1_type":  "string",
				"merge_property_1_value": "string",
				"merge_property_2_type":  "string",
				"merge_property_2_value": "string",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := &schema.Handler{
				Warehouse: warehouseutils.Warehouse{
					Type: tc.destType,
				},
			}
			ms := handler.GetMergeRulesSchema()
			require.Equal(t, ms, tc.expectedSchema)
		})
	}
}

func TestHandler_GetIdentitiesMappingsSchema(t *testing.T) {
	testCases := []struct {
		name           string
		destType       string
		expectedSchema warehouseutils.TableSchema
	}{
		{
			name:     "For snowflake",
			destType: warehouseutils.SNOWFLAKE,
			expectedSchema: warehouseutils.TableSchema{
				"MERGE_PROPERTY_TYPE":  "string",
				"MERGE_PROPERTY_VALUE": "string",
				"RUDDER_ID":            "string",
				"UPDATED_AT":           "datetime",
			},
		},
		{
			name:     "For redshift",
			destType: warehouseutils.RS,
			expectedSchema: warehouseutils.TableSchema{
				"merge_property_type":  "string",
				"merge_property_value": "string",
				"rudder_id":            "string",
				"updated_at":           "datetime",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := &schema.Handler{
				Warehouse: warehouseutils.Warehouse{
					Type: tc.destType,
				},
			}
			ms := handler.GetIdentitiesMappingsSchema()
			require.Equal(t, ms, tc.expectedSchema)
		})
	}
}

func TestHandler_GetDiscardsSchema(t *testing.T) {
	testCases := []struct {
		name           string
		destType       string
		expectedSchema warehouseutils.TableSchema
	}{
		{
			name:     "For snowflake",
			destType: warehouseutils.SNOWFLAKE,
			expectedSchema: warehouseutils.TableSchema{
				"COLUMN_NAME":  "string",
				"COLUMN_VALUE": "string",
				"RECEIVED_AT":  "datetime",
				"ROW_ID":       "string",
				"TABLE_NAME":   "string",
				"UUID_TS":      "datetime",
			},
		},
		{
			name:     "For redshift",
			destType: warehouseutils.RS,
			expectedSchema: warehouseutils.TableSchema{
				"column_name":  "string",
				"column_value": "string",
				"received_at":  "datetime",
				"row_id":       "string",
				"table_name":   "string",
				"uuid_ts":      "datetime",
			},
		},
		{
			name:     "For bigquery",
			destType: warehouseutils.BQ,
			expectedSchema: warehouseutils.TableSchema{
				"column_name":  "string",
				"column_value": "string",
				"loaded_at":    "datetime",
				"received_at":  "datetime",
				"row_id":       "string",
				"table_name":   "string",
				"uuid_ts":      "datetime",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := &schema.Handler{
				Warehouse: warehouseutils.Warehouse{
					Type: tc.destType,
				},
			}
			ds := handler.GetDiscardsSchema()
			require.Equal(t, ds, tc.expectedSchema)
		})
	}
}

func TestHandler_HasSchemaChanged(t *testing.T) {
	testCases := []struct {
		name              string
		localSchema       warehouseutils.Schema
		schemaInWarehouse warehouseutils.Schema
		skipDeepEquals    bool
		expected          bool
	}{
		{
			name:              "When both schemas are empty and skipDeepEquals is true",
			localSchema:       warehouseutils.Schema{},
			schemaInWarehouse: warehouseutils.Schema{},
			skipDeepEquals:    true,
			expected:          false,
		},
		{
			name:        "When local schema is empty and skipDeepEquals is true",
			localSchema: warehouseutils.Schema{},
			schemaInWarehouse: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value",
				},
			},
			skipDeepEquals: true,
			expected:       false,
		},
		{
			name: "same table, same column, different datatype and skipDeepEquals is true",
			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-1",
				},
			},
			schemaInWarehouse: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-2",
				},
			},
			skipDeepEquals: true,
			expected:       true,
		},
		{
			name: "same table, different columns and skipDeepEquals is true",
			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
				},
			},
			schemaInWarehouse: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-2",
				},
			},
			skipDeepEquals: true,
			expected:       true,
		},
		{
			name: "different table and skipDeepEquals is true",
			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "string",
				},
			},
			schemaInWarehouse: warehouseutils.Schema{
				"test-table-1": warehouseutils.TableSchema{
					"test-column": "text",
				},
			},
			skipDeepEquals: true,
			expected:       true,
		},
		{
			name:              "When both schemas are empty and skipDeepEquals is false",
			localSchema:       warehouseutils.Schema{},
			schemaInWarehouse: warehouseutils.Schema{},
			skipDeepEquals:    false,
			expected:          false,
		},
		{
			name:        "When local schema is empty and skipDeepEquals is false",
			localSchema: warehouseutils.Schema{},
			schemaInWarehouse: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value",
				},
			},
			skipDeepEquals: false,
			expected:       true,
		},
		{
			name: "same table, same column, different datatype and skipDeepEquals is false",
			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-1",
				},
			},
			schemaInWarehouse: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-2",
				},
			},
			skipDeepEquals: false,
			expected:       true,
		},
		{
			name: "same table, different columns and skipDeepEquals is false",
			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
				},
			},
			schemaInWarehouse: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "test-value-2",
				},
			},
			skipDeepEquals: false,
			expected:       true,
		},
		{
			name: "different table and skipDeepEquals is false",
			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "string",
				},
			},
			schemaInWarehouse: warehouseutils.Schema{
				"test-table-1": warehouseutils.TableSchema{
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
				Warehouse: warehouseutils.Warehouse{
					Type: warehouseutils.SNOWFLAKE,
				},
				SkipDeepEqualSchemas: tc.skipDeepEquals,
			}
			hasSchemaChanged := handler.HasSchemaChanged(tc.localSchema, tc.schemaInWarehouse)
			require.Equal(t, tc.expected, hasSchemaChanged)
		})
	}
}

func TestMergeSchema(t *testing.T) {
	testCases := []struct {
		name           string
		schemaList     []warehouseutils.Schema
		localSchema    warehouseutils.Schema
		expectedSchema warehouseutils.Schema
	}{
		{
			name: "local schema contains the property and schema list contains data types in text-string order",
			schemaList: []warehouseutils.Schema{
				{
					"test-table": warehouseutils.TableSchema{
						"test-column": "text",
					},
				},
				{
					"test-table": warehouseutils.TableSchema{
						"test-column": "string",
					},
				},
			},
			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "string",
				},
			},
			expectedSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "text",
				},
			},
		},
		{
			name: "local schema contains the property and schema list contains data types in string-text order",
			schemaList: []warehouseutils.Schema{
				{
					"test-table": warehouseutils.TableSchema{
						"test-column": "string",
					},
				},
				{
					"test-table": warehouseutils.TableSchema{
						"test-column": "text",
					},
				},
			},

			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "string",
				},
			},
			expectedSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "text",
				},
			},
		},
		{
			name: "local schema does not contain the property",
			schemaList: []warehouseutils.Schema{
				{
					"test-table": warehouseutils.TableSchema{
						"test-column": "int",
					},
				},
				{
					"test-table": warehouseutils.TableSchema{
						"test-column": "text",
					},
				},
			},
			localSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column-1": "string",
				},
			},
			expectedSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test-column": "int",
				},
			},
		},
	}

	destType := "RS"

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actualSchema := schema.MergeSchema(tc.localSchema, tc.schemaList, warehouseutils.Schema{}, destType)
			require.Equal(t, actualSchema, tc.expectedSchema)
		})
	}
}

func TestSchemaHandleT_SkipDeprecatedColumns(t *testing.T) {
	warehouse.Init4()

	testCases := []struct {
		name           string
		schema         warehouseutils.Schema
		expectedSchema warehouseutils.Schema
	}{
		{
			name: "no deprecated columns",
			schema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
					"test_datetime":  "datetime",
				},
			},
			expectedSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
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
			schema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
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
			expectedSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
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
			schema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
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
			expectedSchema: warehouseutils.Schema{
				"test-table": warehouseutils.TableSchema{
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

			sh := &schema.Handler{
				Warehouse: warehouseutils.Warehouse{
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

			sh.SkipDeprecatedColumns(tc.schema)

			require.Equal(t, tc.schema, tc.expectedSchema)
		})
	}
}
