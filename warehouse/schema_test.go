//go:build !warehouse_integration

package warehouse

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ = Describe("Schema", func() {
	Describe("Handle schema change", func() {
		Context("No discards", func() {
			It("should send int values if existing datatype is int", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				var ok bool

				columnVal = 1.501
				convertedVal = 1
				newColumnVal, ok = HandleSchemaChange("int", "float", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())
			})

			It("should send float values if existing datatype is float", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				var ok bool

				columnVal = 1
				convertedVal = 1.0
				newColumnVal, ok = HandleSchemaChange("float", "int", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())
			})

			It("should send string values if existing datatype is string", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				var ok bool

				columnVal = false
				convertedVal = "false"
				newColumnVal, ok = HandleSchemaChange("string", "boolean", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = 1
				convertedVal = "1"
				newColumnVal, ok = HandleSchemaChange("string", "int", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = 1.501
				convertedVal = "1.501"
				newColumnVal, ok = HandleSchemaChange("string", "float", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = "2022-05-05T00:00:00.000Z"
				convertedVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, ok = HandleSchemaChange("string", "datetime", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = `{"json":true}`
				convertedVal = `{"json":true}`
				newColumnVal, ok = HandleSchemaChange("string", "json", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())
			})

			It("should send json string values if existing datatype is json", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				var ok bool

				columnVal = false
				convertedVal = "false"
				newColumnVal, ok = HandleSchemaChange("json", "boolean", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = 1
				convertedVal = "1"
				newColumnVal, ok = HandleSchemaChange("json", "int", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = 1.501
				convertedVal = "1.501"
				newColumnVal, ok = HandleSchemaChange("json", "float", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = "2022-05-05T00:00:00.000Z"
				convertedVal = `"2022-05-05T00:00:00.000Z"`
				newColumnVal, ok = HandleSchemaChange("json", "datetime", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = "string value"
				convertedVal = `"string value"`
				newColumnVal, ok = HandleSchemaChange("json", "string", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				var columnArrVal []interface{}
				columnArrVal = append(columnArrVal, false, 1, "string value")
				newColumnVal, ok = HandleSchemaChange("json", "string", columnArrVal)
				Expect(newColumnVal).To(Equal(columnArrVal))
				Expect(ok).To(BeTrue())
			})
		})

		Context("Discards", func() {
			It("existing datatype is boolean", func() {
				var newColumnVal, columnVal interface{}
				var ok bool

				columnVal = 1
				newColumnVal, ok = HandleSchemaChange("boolean", "int", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = 1.501
				newColumnVal, ok = HandleSchemaChange("boolean", "float", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "string value"
				newColumnVal, ok = HandleSchemaChange("boolean", "string", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, ok = HandleSchemaChange("boolean", "datetime", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = `{"json":true}`
				newColumnVal, ok = HandleSchemaChange("boolean", "json", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())
			})

			It("existing datatype is int", func() {
				var newColumnVal, columnVal interface{}
				var ok bool

				columnVal = false
				newColumnVal, ok = HandleSchemaChange("int", "boolean", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "string value"
				newColumnVal, ok = HandleSchemaChange("int", "string", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, ok = HandleSchemaChange("int", "datetime", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = `{"json":true}`
				newColumnVal, ok = HandleSchemaChange("int", "json", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())
			})

			It("existing datatype is float", func() {
				var newColumnVal, columnVal interface{}
				var ok bool

				columnVal = false
				newColumnVal, ok = HandleSchemaChange("float", "boolean", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "string value"
				newColumnVal, ok = HandleSchemaChange("float", "string", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, ok = HandleSchemaChange("float", "datetime", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = `{"json":true}`
				newColumnVal, ok = HandleSchemaChange("float", "json", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())
			})

			It("existing datatype is datetime", func() {
				var newColumnVal, columnVal interface{}
				var ok bool
				columnVal = false
				newColumnVal, ok = HandleSchemaChange("datetime", "boolean", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "string value"
				newColumnVal, ok = HandleSchemaChange("datetime", "string", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = 1
				newColumnVal, ok = HandleSchemaChange("datetime", "int", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = 1.501
				newColumnVal, ok = HandleSchemaChange("datetime", "float", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = `{"json":true}`
				newColumnVal, ok = HandleSchemaChange("datetime", "json", columnVal)
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())
			})
		})
	})

	DescribeTable("Get table schema diff", func(tableName string, currentSchema, uploadSchema warehouseutils.SchemaT, expected warehouseutils.TableSchemaDiffT) {
		Expect(getTableSchemaDiff(tableName, currentSchema, uploadSchema)).To(Equal(expected))
	},
		Entry(nil, "test-table", warehouseutils.SchemaT{}, warehouseutils.SchemaT{}, warehouseutils.TableSchemaDiffT{
			ColumnMap:     map[string]string{},
			UpdatedSchema: map[string]string{},
		}),

		Entry(nil, "test-table", warehouseutils.SchemaT{}, warehouseutils.SchemaT{
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
		}),

		Entry(nil, "test-table", warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column": "test-value-1",
			},
		}, warehouseutils.SchemaT{
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
		}),

		Entry(nil, "test-table", warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column-1": "test-value-1",
				"test-column-2": "test-value-2",
			},
		}, warehouseutils.SchemaT{
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
		}),

		Entry(nil, "test-table", warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column":   "string",
				"test-column-2": "test-value-2",
			},
		}, warehouseutils.SchemaT{
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
			StringColumnsToBeAlteredToText: []string{"test-column"},
		}),
	)

	DescribeTable("Merge Upload and Local Schema", func(uploadSchema, schemaInWarehousePreUpload, expected warehouseutils.SchemaT) {
		Expect(mergeUploadAndLocalSchemas(uploadSchema, schemaInWarehousePreUpload)).To(Equal(expected))
	},

		Entry(nil, warehouseutils.SchemaT{}, warehouseutils.SchemaT{}, warehouseutils.SchemaT{}),

		Entry(nil, warehouseutils.SchemaT{}, warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column": "test-value",
			},
		}, warehouseutils.SchemaT{}),

		Entry(nil, warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column": "test-value-1",
			},
		}, warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column": "test-value-2",
			},
		}, warehouseutils.SchemaT{
			"test-table": {
				"test-column": "test-value-2",
			},
		}),

		Entry(nil, warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column-1": "test-value-1",
				"test-column-2": "test-value-2",
			},
		}, warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column": "test-value-2",
			},
		}, warehouseutils.SchemaT{
			"test-table": {
				"test-column":   "test-value-2",
				"test-column-1": "test-value-1",
				"test-column-2": "test-value-2",
			},
		}),

		Entry(nil, warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column":   "string",
				"test-column-2": "test-value-2",
			},
		}, warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column": "text",
			},
		}, warehouseutils.SchemaT{
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
				Init4()
			})

			DescribeTable("Check has schema changed", func(localSchema, schemaInWarehouse warehouseutils.SchemaT, expected bool) {
				Expect(hasSchemaChanged(localSchema, schemaInWarehouse)).To(Equal(expected))
			},

				Entry(nil, warehouseutils.SchemaT{}, warehouseutils.SchemaT{}, false),

				Entry(nil, warehouseutils.SchemaT{}, warehouseutils.SchemaT{
					"test-table": map[string]string{
						"test-column": "test-value",
					},
				}, false),

				Entry(nil, warehouseutils.SchemaT{
					"test-table": map[string]string{
						"test-column": "test-value-1",
					},
				}, warehouseutils.SchemaT{
					"test-table": map[string]string{
						"test-column": "test-value-2",
					},
				}, true),

				Entry(nil, warehouseutils.SchemaT{
					"test-table": map[string]string{
						"test-column-1": "test-value-1",
						"test-column-2": "test-value-2",
					},
				}, warehouseutils.SchemaT{
					"test-table": map[string]string{
						"test-column": "test-value-2",
					},
				}, true),

				Entry(nil, warehouseutils.SchemaT{
					"test-table": map[string]string{
						"test-column": "string",
					},
				}, warehouseutils.SchemaT{
					"test-table-1": map[string]string{
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

			DescribeTable("Check has schema changed", func(localSchema, schemaInWarehouse warehouseutils.SchemaT, expected bool) {
				Expect(hasSchemaChanged(localSchema, schemaInWarehouse)).To(Equal(expected))
			},

				Entry(nil, warehouseutils.SchemaT{}, warehouseutils.SchemaT{}, false),

				Entry(nil, warehouseutils.SchemaT{}, warehouseutils.SchemaT{
					"test-table": map[string]string{
						"test-column": "test-value",
					},
				}, true),
			)
		})
	})

	DescribeTable("Safe name", func(warehouseType, columnName, expected string) {
		handle := SchemaHandleT{
			warehouse: warehouseutils.WarehouseT{
				Type: warehouseType,
			},
		}
		Expect(handle.safeName(columnName)).To(Equal(expected))
	},
		Entry(nil, "BQ", "test-column", "test-column"),
		Entry(nil, "SNOWFLAKE", "test-column", "TEST-COLUMN"),
	)

	DescribeTable("Merge rules schema", func(warehouseType string, expected map[string]string) {
		handle := SchemaHandleT{
			warehouse: warehouseutils.WarehouseT{
				Type: warehouseType,
			},
		}
		Expect(handle.getMergeRulesSchema()).To(Equal(expected))
	},
		Entry(nil, "BQ", map[string]string{
			"merge_property_1_type":  "string",
			"merge_property_1_value": "string",
			"merge_property_2_type":  "string",
			"merge_property_2_value": "string",
		}),
		Entry(nil, "SNOWFLAKE", map[string]string{
			"MERGE_PROPERTY_1_TYPE":  "string",
			"MERGE_PROPERTY_1_VALUE": "string",
			"MERGE_PROPERTY_2_TYPE":  "string",
			"MERGE_PROPERTY_2_VALUE": "string",
		}),
	)

	DescribeTable("Identities Mappings schema", func(warehouseType string, expected map[string]string) {
		handle := SchemaHandleT{
			warehouse: warehouseutils.WarehouseT{
				Type: warehouseType,
			},
		}
		Expect(handle.getIdentitiesMappingsSchema()).To(Equal(expected))
	},
		Entry(nil, "BQ", map[string]string{
			"merge_property_type":  "string",
			"merge_property_value": "string",
			"rudder_id":            "string",
			"updated_at":           "datetime",
		}),
		Entry(nil, "SNOWFLAKE", map[string]string{
			"MERGE_PROPERTY_TYPE":  "string",
			"MERGE_PROPERTY_VALUE": "string",
			"RUDDER_ID":            "string",
			"UPDATED_AT":           "datetime",
		}),
	)

	DescribeTable("Discards schema", func(warehouseType string, expected map[string]string) {
		handle := SchemaHandleT{
			warehouse: warehouseutils.WarehouseT{
				Type: warehouseType,
			},
		}
		Expect(handle.getDiscardsSchema()).To(Equal(expected))
	},
		Entry(nil, "BQ", map[string]string{
			"table_name":   "string",
			"row_id":       "string",
			"column_name":  "string",
			"column_value": "string",
			"received_at":  "datetime",
			"uuid_ts":      "datetime",
			"loaded_at":    "datetime",
		}),
		Entry(nil, "SNOWFLAKE", map[string]string{
			"TABLE_NAME":   "string",
			"ROW_ID":       "string",
			"COLUMN_NAME":  "string",
			"COLUMN_VALUE": "string",
			"RECEIVED_AT":  "datetime",
			"UUID_TS":      "datetime",
		}),
	)

	DescribeTable("Merge schema", func(currentSchema warehouseutils.SchemaT, schemaList []warehouseutils.SchemaT, currentMergedSchema warehouseutils.SchemaT, warehouseType string, expected warehouseutils.SchemaT) {
		Expect(mergeSchema(currentSchema, schemaList, currentMergedSchema, warehouseType)).To(Equal(expected))
	},
		Entry(nil, warehouseutils.SchemaT{}, []warehouseutils.SchemaT{}, warehouseutils.SchemaT{}, "BQ", warehouseutils.SchemaT{}),
		Entry(nil, warehouseutils.SchemaT{}, []warehouseutils.SchemaT{
			{
				"test-table": map[string]string{
					"test-column": "test-value",
				},
			},
		}, warehouseutils.SchemaT{}, "BQ", warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column": "test-value",
			},
		}),
		Entry(nil, warehouseutils.SchemaT{}, []warehouseutils.SchemaT{
			{
				"users": map[string]string{
					"test-column": "test-value",
				},
				"identifies": map[string]string{
					"test-column": "test-value",
				},
			},
		}, warehouseutils.SchemaT{}, "BQ", warehouseutils.SchemaT{
			"users": map[string]string{
				"test-column": "test-value",
			},
			"identifies": map[string]string{
				"test-column": "test-value",
			},
		}),
		Entry(nil, warehouseutils.SchemaT{
			"users": map[string]string{
				"test-column": "test-value",
			},
			"identifies": map[string]string{
				"test-column": "test-value",
			},
		}, []warehouseutils.SchemaT{
			{
				"users": map[string]string{
					"test-column": "test-value",
				},
				"identifies": map[string]string{
					"test-column": "test-value",
				},
			},
		}, warehouseutils.SchemaT{}, "BQ", warehouseutils.SchemaT{
			"users": map[string]string{
				"test-column": "test-value",
			},
			"identifies": map[string]string{
				"test-column": "test-value",
			},
		}),
		Entry(nil, warehouseutils.SchemaT{}, []warehouseutils.SchemaT{
			{
				"test-table": map[string]string{
					"test-column":   "string",
					"test-column-2": "test-value-2",
				},
			},
		}, warehouseutils.SchemaT{
			"test-table": map[string]string{
				"test-column": "text",
			},
		}, "BQ", warehouseutils.SchemaT{
			"test-table": {
				"test-column":   "text",
				"test-column-2": "test-value-2",
			},
		}),
	)
})
