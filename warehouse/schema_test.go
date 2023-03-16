package warehouse

import (
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is float",
			existingDatatype: "boolean",
			currentDataType:  "float",
			value:            1.501,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is string",
			existingDatatype: "boolean",
			currentDataType:  "string",
			value:            "string value",
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is datetime",
			existingDatatype: "boolean",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is boolean, new datatype is json",
			existingDatatype: "boolean",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is boolean",
			existingDatatype: "int",
			currentDataType:  "boolean",
			value:            false,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is string",
			existingDatatype: "int",
			currentDataType:  "string",
			value:            "string value",
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is datetime",
			existingDatatype: "int",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is json",
			existingDatatype: "int",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is int, new datatype is float",
			existingDatatype: "int",
			currentDataType:  "float",
			value:            1,
			convError:        ErrIncompatibleSchemaConversion,
		},
		{
			name:             "existing datatype is float, new datatype is boolean",
			existingDatatype: "float",
			currentDataType:  "boolean",
			value:            false,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is int",
			existingDatatype: "float",
			currentDataType:  "int",
			value:            1.0,
			convError:        ErrIncompatibleSchemaConversion,
		},
		{
			name:             "existing datatype is float, new datatype is string",
			existingDatatype: "float",
			currentDataType:  "string",
			value:            "string value",
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is datetime",
			existingDatatype: "float",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is float, new datatype is json",
			existingDatatype: "float",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is boolean",
			existingDatatype: "datetime",
			currentDataType:  "boolean",
			value:            false,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is string",
			existingDatatype: "datetime",
			currentDataType:  "string",
			value:            "string value",
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is int",
			existingDatatype: "datetime",
			currentDataType:  "int",
			value:            1,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is float",
			existingDatatype: "datetime",
			currentDataType:  "float",
			value:            1.501,
			convError:        ErrSchemaConversionNotSupported,
		},
		{
			name:             "existing datatype is datetime, new datatype is json",
			existingDatatype: "datetime",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        ErrSchemaConversionNotSupported,
		},
	}
	for _, ip := range inputs {
		tc := ip

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			newColumnVal, convError := HandleSchemaChange(
				model.SchemaType(tc.existingDatatype),
				model.SchemaType(tc.currentDataType),
				tc.value,
			)
			require.Equal(t, newColumnVal, tc.newColumnVal)
			require.Equal(t, convError, tc.convError)
		})
	}
}

var _ = Describe("Schema", func() {
	DescribeTable("Get table schema diff", func(tableName string, currentSchema, uploadSchema model.Schema, expected warehouseutils.TableSchemaDiff) {
		Expect(getTableSchemaDiff(tableName, currentSchema, uploadSchema)).To(BeEquivalentTo(expected))
	},
		Entry(nil, "test-table", model.Schema{}, model.Schema{}, warehouseutils.TableSchemaDiff{
			ColumnMap:        model.TableSchema{},
			UpdatedSchema:    model.TableSchema{},
			AlteredColumnMap: model.TableSchema{},
		}),

		Entry(nil, "test-table", model.Schema{}, model.Schema{
			"test-table": model.TableSchema{
				"test-column": "test-value",
			},
		}, warehouseutils.TableSchemaDiff{
			Exists:           true,
			TableToBeCreated: true,
			ColumnMap: model.TableSchema{
				"test-column": "test-value",
			},
			UpdatedSchema: model.TableSchema{
				"test-column": "test-value",
			},
			AlteredColumnMap: model.TableSchema{},
		}),

		Entry(nil, "test-table", model.Schema{
			"test-table": model.TableSchema{
				"test-column": "test-value-1",
			},
		}, model.Schema{
			"test-table": model.TableSchema{
				"test-column": "test-value-2",
			},
		}, warehouseutils.TableSchemaDiff{
			Exists:           false,
			TableToBeCreated: false,
			ColumnMap:        model.TableSchema{},
			UpdatedSchema: model.TableSchema{
				"test-column": "test-value-1",
			},
			AlteredColumnMap: model.TableSchema{},
		}),

		Entry(nil, "test-table", model.Schema{
			"test-table": model.TableSchema{
				"test-column-1": "test-value-1",
				"test-column-2": "test-value-2",
			},
		}, model.Schema{
			"test-table": model.TableSchema{
				"test-column": "test-value-2",
			},
		}, warehouseutils.TableSchemaDiff{
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
		}),

		Entry(nil, "test-table", model.Schema{
			"test-table": model.TableSchema{
				"test-column":   "string",
				"test-column-2": "test-value-2",
			},
		}, model.Schema{
			"test-table": model.TableSchema{
				"test-column": "text",
			},
		}, warehouseutils.TableSchemaDiff{
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
		}),
	)

	Describe("Has schema changed", func() {
		g := GinkgoT()

		Context("when skipping deep equals", func() {
			BeforeEach(func() {
				g.Setenv("RSERVER_WAREHOUSE_SKIP_DEEP_EQUAL_SCHEMAS", "true")
				Init4()
			})

			DescribeTable("Check has schema changed", func(localSchema, schemaInWarehouse model.Schema, expected bool) {
				Expect(hasSchemaChanged(localSchema, schemaInWarehouse)).To(Equal(expected))
			},

				Entry(nil, model.Schema{}, model.Schema{}, false),

				Entry(nil, model.Schema{}, model.Schema{
					"test-table": model.TableSchema{
						"test-column": "test-value",
					},
				}, false),

				Entry(nil, model.Schema{
					"test-table": model.TableSchema{
						"test-column": "test-value-1",
					},
				}, model.Schema{
					"test-table": model.TableSchema{
						"test-column": "test-value-2",
					},
				}, true),

				Entry(nil, model.Schema{
					"test-table": model.TableSchema{
						"test-column-1": "test-value-1",
						"test-column-2": "test-value-2",
					},
				}, model.Schema{
					"test-table": model.TableSchema{
						"test-column": "test-value-2",
					},
				}, true),

				Entry(nil, model.Schema{
					"test-table": model.TableSchema{
						"test-column": "string",
					},
				}, model.Schema{
					"test-table-1": model.TableSchema{
						"test-column": "text",
					},
				}, true),
			)
		})

		Context("when not skipping deep equals", func() {
			BeforeEach(func() {
				g.Setenv("RSERVER_WAREHOUSE_SKIP_DEEP_EQUAL_SCHEMAS", "false")
				Init4()
			})

			DescribeTable("Check has schema changed", func(localSchema, schemaInWarehouse model.Schema, expected bool) {
				Expect(hasSchemaChanged(localSchema, schemaInWarehouse)).To(Equal(expected))
			},

				Entry(nil, model.Schema{}, model.Schema{}, false),

				Entry(nil, model.Schema{}, model.Schema{
					"test-table": model.TableSchema{
						"test-column": "test-value",
					},
				}, true),
			)
		})
	})

	DescribeTable("Safe name", func(warehouseType, columnName, expected string) {
		handle := SchemaHandle{
			warehouse: model.Warehouse{
				Type: warehouseType,
			},
		}
		Expect(handle.safeName(columnName)).To(Equal(expected))
	},
		Entry(nil, "BQ", "test-column", "test-column"),
		Entry(nil, "SNOWFLAKE", "test-column", "TEST-COLUMN"),
	)

	DescribeTable("Merge rules schema", func(warehouseType string, expected model.TableSchema) {
		handle := SchemaHandle{
			warehouse: model.Warehouse{
				Type: warehouseType,
			},
		}
		Expect(handle.getMergeRulesSchema()).To(Equal(expected))
	},
		Entry(nil, "BQ", model.TableSchema{
			"merge_property_1_type":  "string",
			"merge_property_1_value": "string",
			"merge_property_2_type":  "string",
			"merge_property_2_value": "string",
		}),
		Entry(nil, "SNOWFLAKE", model.TableSchema{
			"MERGE_PROPERTY_1_TYPE":  "string",
			"MERGE_PROPERTY_1_VALUE": "string",
			"MERGE_PROPERTY_2_TYPE":  "string",
			"MERGE_PROPERTY_2_VALUE": "string",
		}),
	)

	DescribeTable("Identities Mappings schema", func(warehouseType string, expected model.TableSchema) {
		handle := SchemaHandle{
			warehouse: model.Warehouse{
				Type: warehouseType,
			},
		}
		Expect(handle.getIdentitiesMappingsSchema()).To(Equal(expected))
	},
		Entry(nil, "BQ", model.TableSchema{
			"merge_property_type":  "string",
			"merge_property_value": "string",
			"rudder_id":            "string",
			"updated_at":           "datetime",
		}),
		Entry(nil, "SNOWFLAKE", model.TableSchema{
			"MERGE_PROPERTY_TYPE":  "string",
			"MERGE_PROPERTY_VALUE": "string",
			"RUDDER_ID":            "string",
			"UPDATED_AT":           "datetime",
		}),
	)

	DescribeTable("Discards schema", func(warehouseType string, expected model.TableSchema) {
		handle := SchemaHandle{
			warehouse: model.Warehouse{
				Type: warehouseType,
			},
		}
		Expect(handle.getDiscardsSchema()).To(Equal(expected))
	},
		Entry(nil, "BQ", model.TableSchema{
			"table_name":   "string",
			"row_id":       "string",
			"column_name":  "string",
			"column_value": "string",
			"received_at":  "datetime",
			"uuid_ts":      "datetime",
			"loaded_at":    "datetime",
		}),
		Entry(nil, "SNOWFLAKE", model.TableSchema{
			"TABLE_NAME":   "string",
			"ROW_ID":       "string",
			"COLUMN_NAME":  "string",
			"COLUMN_VALUE": "string",
			"RECEIVED_AT":  "datetime",
			"UUID_TS":      "datetime",
		}),
	)

	DescribeTable("Merge schema", func(currentSchema model.Schema, schemaList []model.Schema, currentMergedSchema model.Schema, warehouseType string, expected model.Schema) {
		Expect(MergeSchema(currentSchema, schemaList, currentMergedSchema, warehouseType)).To(Equal(expected))
	},
		Entry(nil, model.Schema{}, []model.Schema{}, model.Schema{}, "BQ", model.Schema{}),
		Entry(nil, model.Schema{}, []model.Schema{
			{
				"test-table": model.TableSchema{
					"test-column": "test-value",
				},
			},
		}, model.Schema{}, "BQ", model.Schema{
			"test-table": model.TableSchema{
				"test-column": "test-value",
			},
		}),
		Entry(nil, model.Schema{}, []model.Schema{
			{
				"users": model.TableSchema{
					"test-column": "test-value",
				},
				"identifies": model.TableSchema{
					"test-column": "test-value",
				},
			},
		}, model.Schema{}, "BQ", model.Schema{
			"users": model.TableSchema{
				"test-column": "test-value",
			},
			"identifies": model.TableSchema{
				"test-column": "test-value",
			},
		}),
		Entry(nil, model.Schema{
			"users": model.TableSchema{
				"test-column": "test-value",
			},
			"identifies": model.TableSchema{
				"test-column": "test-value",
			},
		}, []model.Schema{
			{
				"users": model.TableSchema{
					"test-column": "test-value",
				},
				"identifies": model.TableSchema{
					"test-column": "test-value",
				},
			},
		}, model.Schema{}, "BQ", model.Schema{
			"users": model.TableSchema{
				"test-column": "test-value",
			},
			"identifies": model.TableSchema{
				"test-column": "test-value",
			},
		}),
		Entry(nil, model.Schema{}, []model.Schema{
			{
				"test-table": model.TableSchema{
					"test-column":   "string",
					"test-column-2": "test-value-2",
				},
			},
		}, model.Schema{
			"test-table": model.TableSchema{
				"test-column": "text",
			},
		}, "BQ", model.Schema{
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
		schemaList     []model.Schema
		localSchema    model.Schema
		expectedSchema model.Schema
	}{
		{
			name: "local schema contains the property and schema list contains data types in text-string order",
			schemaList: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test-column": "text",
					},
				},
				{
					"test-table": model.TableSchema{
						"test-column": "string",
					},
				},
			},
			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "string",
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "text",
				},
			},
		},
		{
			name: "local schema contains the property and schema list contains data types in string-text order",
			schemaList: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test-column": "string",
					},
				},
				{
					"test-table": model.TableSchema{
						"test-column": "text",
					},
				},
			},

			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "string",
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "text",
				},
			},
		},
		{
			name: "local schema does not contain the property",
			schemaList: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test-column": "int",
					},
				},
				{
					"test-table": model.TableSchema{
						"test-column": "text",
					},
				},
			},
			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column-1": "string",
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
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

			actualSchema := MergeSchema(tc.localSchema, tc.schemaList, model.Schema{}, destType)
			require.Equal(t, actualSchema, tc.expectedSchema)
		})
	}
}

func TestSchemaHandleT_SkipDeprecatedColumns(t *testing.T) {
	Init4()

	testCases := []struct {
		name           string
		schema         model.Schema
		expectedSchema model.Schema
	}{
		{
			name: "no deprecated columns",
			schema: model.Schema{
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
			schema: model.Schema{
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
			schema: model.Schema{
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

			sh := &SchemaHandle{
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
			}

			sh.SkipDeprecatedColumns(tc.schema)

			require.Equal(t, tc.schema, tc.expectedSchema)
		})
	}
}
