package schema_test

import (
	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/errors"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is float",
			existingDatatype: "boolean",
			currentDataType:  "float",
			value:            1.501,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is string",
			existingDatatype: "boolean",
			currentDataType:  "string",
			value:            "string value",
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is datetime",
			existingDatatype: "boolean",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is json",
			existingDatatype: "boolean",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is boolean",
			existingDatatype: "int",
			currentDataType:  "boolean",
			value:            false,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is string",
			existingDatatype: "int",
			currentDataType:  "string",
			value:            "string value",
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is datetime",
			existingDatatype: "int",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is json",
			existingDatatype: "int",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is float",
			existingDatatype: "int",
			currentDataType:  "float",
			value:            1,
			convError:        errors.ErrIncompatibleSchemaConversion,
		},
		{
			name:             "existing datatype is float, new datatype is boolean",
			existingDatatype: "float",
			currentDataType:  "boolean",
			value:            false,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is int",
			existingDatatype: "float",
			currentDataType:  "int",
			value:            1.0,
			convError:        errors.ErrIncompatibleSchemaConversion,
		},
		{
			name:             "existing datatype is float, new datatype is string",
			existingDatatype: "float",
			currentDataType:  "string",
			value:            "string value",
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is datetime",
			existingDatatype: "float",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is json",
			existingDatatype: "float",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is boolean",
			existingDatatype: "datetime",
			currentDataType:  "boolean",
			value:            false,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is string",
			existingDatatype: "datetime",
			currentDataType:  "string",
			value:            "string value",
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is int",
			existingDatatype: "datetime",
			currentDataType:  "int",
			value:            1,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is float",
			existingDatatype: "datetime",
			currentDataType:  "float",
			value:            1.501,
			convError:        errors.ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is json",
			existingDatatype: "datetime",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        errors.ErrSchemaConversionNotSupported,
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

func TestHandler_MergeRulesSchema(t *testing.T) {
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
			ms := handler.MergeRulesSchema()
			require.Equal(t, ms, tc.expectedSchema)
		})
	}
}

func TestHandler_IdentitiesMappingsSchema(t *testing.T) {
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
			ms := handler.IdentitiesMappingsSchema()
			require.Equal(t, ms, tc.expectedSchema)
		})
	}
}

func TestHandler_DiscardsSchema(t *testing.T) {
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
			ds := handler.DiscardsSchema()
			require.Equal(t, ds, tc.expectedSchema)
		})
	}
}

var _ = Describe("Schema", func() {
	DescribeTable("Get table schema diff", func(tableName string, currentSchema, uploadSchema warehouseutils.Schema, expected warehouseutils.TableSchemaDiffT) {
		Expect(schema.GetTableSchemaDiff(tableName, currentSchema, uploadSchema)).To(BeEquivalentTo(expected))
	},
		Entry(nil, "test-table", warehouseutils.Schema{}, warehouseutils.Schema{}, warehouseutils.TableSchemaDiffT{
			ColumnMap:        map[string]string{},
			UpdatedSchema:    map[string]string{},
			AlteredColumnMap: map[string]string{},
		}),

		Entry(nil, "test-table", warehouseutils.Schema{}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value",
			},
		}, warehouseutils.TableSchemaDiffT{
			Exists:           true,
			TableToBeCreated: true,
			ColumnMap: map[string]string{
				"test-column": "test-value",
			},
			UpdatedSchema: map[string]string{
				"test-column": "test-value",
			},
			AlteredColumnMap: map[string]string{},
		}),

		Entry(nil, "test-table", warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value-1",
			},
		}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value-2",
			},
		}, warehouseutils.TableSchemaDiffT{
			Exists:           false,
			TableToBeCreated: false,
			ColumnMap:        map[string]string{},
			UpdatedSchema: map[string]string{
				"test-column": "test-value-1",
			},
			AlteredColumnMap: map[string]string{},
		}),

		Entry(nil, "test-table", warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column-1": "test-value-1",
				"test-column-2": "test-value-2",
			},
		}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value-2",
			},
		}, warehouseutils.TableSchemaDiffT{
			Exists:           true,
			TableToBeCreated: false,
			ColumnMap: map[string]string{
				"test-column": "test-value-2",
			},
			UpdatedSchema: map[string]string{
				"test-column-1": "test-value-1",
				"test-column-2": "test-value-2",
				"test-column":   "test-value-2",
			},
			AlteredColumnMap: map[string]string{},
		}),

		Entry(nil, "test-table", warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column":   "string",
				"test-column-2": "test-value-2",
			},
		}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "text",
			},
		}, warehouseutils.TableSchemaDiffT{
			Exists:           true,
			TableToBeCreated: false,
			ColumnMap:        map[string]string{},
			UpdatedSchema: map[string]string{
				"test-column-2": "test-value-2",
				"test-column":   "text",
			},
			AlteredColumnMap: map[string]string{
				"test-column": "text",
			},
		}),
	)

	DescribeTable("Merge Upload and Local Schema", func(uploadSchema, schemaInWarehousePreUpload, expected warehouseutils.Schema) {
		Expect(schema.MergeUploadAndLocalSchemas(uploadSchema, schemaInWarehousePreUpload)).To(Equal(expected))
	},

		Entry(nil, warehouseutils.Schema{}, warehouseutils.Schema{}, warehouseutils.Schema{}),

		Entry(nil, warehouseutils.Schema{}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value",
			},
		}, warehouseutils.Schema{}),

		Entry(nil, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value-1",
			},
		}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value-2",
			},
		}, warehouseutils.Schema{
			"test-table": {
				"test-column": "test-value-2",
			},
		}),

		Entry(nil, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column-1": "test-value-1",
				"test-column-2": "test-value-2",
			},
		}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value-2",
			},
		}, warehouseutils.Schema{
			"test-table": {
				"test-column":   "test-value-2",
				"test-column-1": "test-value-1",
				"test-column-2": "test-value-2",
			},
		}),

		Entry(nil, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column":   "string",
				"test-column-2": "test-value-2",
			},
		}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "text",
			},
		}, warehouseutils.Schema{
			"test-table": {
				"test-column-2": "test-value-2",
				"test-column":   "text",
			},
		}),
	)

	Describe("Has schema changed", func() {
		g := GinkgoT()

		Context("when skipping deep equals", func() {
			BeforeEach(func() {
				g.Setenv("RSERVER_WAREHOUSE_SKIP_DEEP_EQUAL_SCHEMAS", "true")
				warehouse.Init4()
			})

			DescribeTable("Check has schema changed", func(localSchema, schemaInWarehouse warehouseutils.Schema, expected bool) {
				handler := schema.Handler{}
				Expect(handler.HasSchemaChanged(localSchema, schemaInWarehouse)).To(Equal(expected))
			},

				Entry(nil, warehouseutils.Schema{}, warehouseutils.Schema{}, false),

				Entry(nil, warehouseutils.Schema{}, warehouseutils.Schema{
					"test-table": map[string]string{
						"test-column": "test-value",
					},
				}, false),

				Entry(nil, warehouseutils.Schema{
					"test-table": map[string]string{
						"test-column": "test-value-1",
					},
				}, warehouseutils.Schema{
					"test-table": map[string]string{
						"test-column": "test-value-2",
					},
				}, true),

				Entry(nil, warehouseutils.Schema{
					"test-table": map[string]string{
						"test-column-1": "test-value-1",
						"test-column-2": "test-value-2",
					},
				}, warehouseutils.Schema{
					"test-table": map[string]string{
						"test-column": "test-value-2",
					},
				}, true),

				Entry(nil, warehouseutils.Schema{
					"test-table": map[string]string{
						"test-column": "string",
					},
				}, warehouseutils.Schema{
					"test-table-1": map[string]string{
						"test-column": "text",
					},
				}, true),
			)
		})

		Context("when not skipping deep equals", func() {
			BeforeEach(func() {
				g.Setenv("RSERVER_WAREHOUSE_SKIP_DEEP_EQUAL_SCHEMAS", "false")
				warehouse.Init4()
			})

			DescribeTable("Check has schema changed", func(localSchema, schemaInWarehouse warehouseutils.Schema, expected bool) {
				handle := schema.Handler{}
				Expect(handle.HasSchemaChanged(localSchema, schemaInWarehouse)).To(Equal(expected))
			},

				Entry(nil, warehouseutils.Schema{}, warehouseutils.Schema{}, false),

				Entry(nil, warehouseutils.Schema{}, warehouseutils.Schema{
					"test-table": map[string]string{
						"test-column": "test-value",
					},
				}, true),
			)
		})
	})

	DescribeTable("Safe name", func(warehouseType, columnName, expected string) {
		handle := schema.Handler{
			Warehouse: warehouseutils.Warehouse{
				Type: warehouseType,
			},
		}
		Expect(handle.SafeName(columnName)).To(Equal(expected))
	},
		Entry(nil, "BQ", "test-column", "test-column"),
		Entry(nil, "SNOWFLAKE", "test-column", "TEST-COLUMN"),
	)

	DescribeTable("Merge schema", func(currentSchema warehouseutils.Schema, schemaList []warehouseutils.Schema, currentMergedSchema warehouseutils.Schema, warehouseType string, expected warehouseutils.Schema) {
		Expect(schema.MergeSchema(currentSchema, schemaList, currentMergedSchema, warehouseType)).To(Equal(expected))
	},
		Entry(nil, warehouseutils.Schema{}, []warehouseutils.Schema{}, warehouseutils.Schema{}, "BQ", warehouseutils.Schema{}),
		Entry(nil, warehouseutils.Schema{}, []warehouseutils.Schema{
			{
				"test-table": map[string]string{
					"test-column": "test-value",
				},
			},
		}, warehouseutils.Schema{}, "BQ", warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "test-value",
			},
		}),
		Entry(nil, warehouseutils.Schema{}, []warehouseutils.Schema{
			{
				"users": map[string]string{
					"test-column": "test-value",
				},
				"identifies": map[string]string{
					"test-column": "test-value",
				},
			},
		}, warehouseutils.Schema{}, "BQ", warehouseutils.Schema{
			"users": map[string]string{
				"test-column": "test-value",
			},
			"identifies": map[string]string{
				"test-column": "test-value",
			},
		}),
		Entry(nil, warehouseutils.Schema{
			"users": map[string]string{
				"test-column": "test-value",
			},
			"identifies": map[string]string{
				"test-column": "test-value",
			},
		}, []warehouseutils.Schema{
			{
				"users": map[string]string{
					"test-column": "test-value",
				},
				"identifies": map[string]string{
					"test-column": "test-value",
				},
			},
		}, warehouseutils.Schema{}, "BQ", warehouseutils.Schema{
			"users": map[string]string{
				"test-column": "test-value",
			},
			"identifies": map[string]string{
				"test-column": "test-value",
			},
		}),
		Entry(nil, warehouseutils.Schema{}, []warehouseutils.Schema{
			{
				"test-table": map[string]string{
					"test-column":   "string",
					"test-column-2": "test-value-2",
				},
			},
		}, warehouseutils.Schema{
			"test-table": map[string]string{
				"test-column": "text",
			},
		}, "BQ", warehouseutils.Schema{
			"test-table": {
				"test-column":   "text",
				"test-column-2": "test-value-2",
			},
		}),
	)
})

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
					"test-table": map[string]string{
						"test-column": "text",
					},
				},
				{
					"test-table": map[string]string{
						"test-column": "string",
					},
				},
			},
			localSchema: warehouseutils.Schema{
				"test-table": map[string]string{
					"test-column": "string",
				},
			},
			expectedSchema: warehouseutils.Schema{
				"test-table": map[string]string{
					"test-column": "text",
				},
			},
		},
		{
			name: "local schema contains the property and schema list contains data types in string-text order",
			schemaList: []warehouseutils.Schema{
				{
					"test-table": map[string]string{
						"test-column": "string",
					},
				},
				{
					"test-table": map[string]string{
						"test-column": "text",
					},
				},
			},

			localSchema: warehouseutils.Schema{
				"test-table": map[string]string{
					"test-column": "string",
				},
			},
			expectedSchema: warehouseutils.Schema{
				"test-table": map[string]string{
					"test-column": "text",
				},
			},
		},
		{
			name: "local schema does not contain the property",
			schemaList: []warehouseutils.Schema{
				{
					"test-table": map[string]string{
						"test-column": "int",
					},
				},
				{
					"test-table": map[string]string{
						"test-column": "text",
					},
				},
			},
			localSchema: warehouseutils.Schema{
				"test-table": map[string]string{
					"test-column-1": "string",
				},
			},
			expectedSchema: warehouseutils.Schema{
				"test-table": map[string]string{
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
