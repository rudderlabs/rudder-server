package warehouse_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/rudderlabs/rudder-server/warehouse"
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

	Describe("Excluded schema", func() {
		uploadSchema := warehouseutils.SchemaT{
			"demo_event": {
				"anonymous_id":       "string",
				"channel":            "string",
				"context_ip":         "string",
				"context_passed_ip":  "string",
				"context_request_ip": "string",
				"event_text":         "string",
				"group_id":           "string",
				"id":                 "string",
				"loaded_at":          "datetime",
				"name":               "string",
				"original_timestamp": "datetime",
				"previous_id":        "string",
				"received_at":        "datetime",
				"record_id":          "string",
				"sent_at":            "datetime",
				"timestamp":          "datetime",
				"user_id":            "string",
				"uuid_ts":            "datetime", // end of 18 rudder columns
				"ad_name":            "string",
				"blendo_id":          "string",
				"clicks":             "int",
				"currency":           "float",
				"date_start":         "datatime",
				"sample_event":       "json",
			},
		}

		schemaInWarehouse := warehouseutils.SchemaT{
			"demo_event": {
				"anonymous_id":       "string",
				"channel":            "string",
				"context_ip":         "string",
				"context_passed_ip":  "string",
				"context_request_ip": "string",
				"event_text":         "string",
				"group_id":           "string",
				"id":                 "string",
				"loaded_at":          "datetime",
				"name":               "string",
				"original_timestamp": "datetime",
				"previous_id":        "string",
				"received_at":        "datetime",
				"record_id":          "string",
				"sent_at":            "datetime",
				"timestamp":          "datetime",
				"user_id":            "string",
				"uuid_ts":            "datetime", // end of 18 rudder columns
				"ad_name":            "string",
				"blendo_id":          "string",
			},
		}
		// With max col limit: 20
		ExcludedSchema := warehouseutils.SchemaT{
			"demo_event": {
				"date_start":   "datatime",
				"sample_event": "json",
			},
		}

		It("Should not contain excluded schema if max column count is not reached", func() {
			excludedSchema := GetExcludedSchema(uploadSchema, schemaInWarehouse, 1000)
			Expect(excludedSchema).To(Equal(warehouseutils.SchemaT{}))
		})

		It("Should contain excluded schema if max column count is reached and picked in sorted order", func() {
			excludedSchema := GetExcludedSchema(uploadSchema, schemaInWarehouse, 22)
			Expect(excludedSchema).To(Equal(ExcludedSchema))
		})

		It("Should contain rudder reserved columns in included column list if it is a new event and max limit is reached", func() {
			delete(schemaInWarehouse, "demo_event")
			excludedSchema := GetExcludedSchema(uploadSchema, schemaInWarehouse, 22)
			Expect(excludedSchema).To(Equal(ExcludedSchema))
		})
	})

	Describe("Table schema diff", func() {
		tableName := "demo_event"
		uploadSchema := warehouseutils.SchemaT{
			tableName: {
				"id":        "int",
				"record_id": "string",
				"sent_at":   "datetime",
				"name":      "text",
				"timestamp": "datetime",
			},
		}

		Context("Without Excluded schema", func() {
			It("Should contain upload schema as diff for a new event", func() {
				var excludedSchema, currentSchema warehouseutils.SchemaT
				diff := warehouseutils.TableSchemaDiffT{
					Exists:           true,
					TableToBeCreated: true,
					ColumnMap:        uploadSchema[tableName],
					UpdatedSchema:    uploadSchema[tableName],
				}
				tableSchemaDiff := GetTableSchemaDiff(tableName, currentSchema, uploadSchema, excludedSchema)
				Expect(tableSchemaDiff).To(Equal(diff))
			})

			It("Should compute diff schema for an existing event", func() {
				var excludedSchema warehouseutils.SchemaT
				currentSchema := uploadSchema
				diff := warehouseutils.TableSchemaDiffT{
					ColumnMap:     map[string]string{},
					UpdatedSchema: currentSchema[tableName],
				}
				// No diff if all columns are present in warehouse schema
				tableSchemaDiff := GetTableSchemaDiff(tableName, currentSchema, uploadSchema, excludedSchema)
				Expect(tableSchemaDiff).To(Equal(diff))

				currentSchema = warehouseutils.SchemaT{
					tableName: {
						"id":        "string",
						"record_id": "string",
						"sent_at":   "datetime",
					},
				}
				diff = warehouseutils.TableSchemaDiffT{
					Exists: true,
					ColumnMap: map[string]string{
						"name":      "text",
						"timestamp": "datetime",
					},
					UpdatedSchema: map[string]string{
						"id":        "string", // currentSchema has more preference
						"record_id": "string",
						"sent_at":   "datetime",
						"name":      "text",
						"timestamp": "datetime",
					},
				}
				tableSchemaDiff = GetTableSchemaDiff(tableName, currentSchema, uploadSchema, excludedSchema)
				Expect(tableSchemaDiff).To(Equal(diff))
			})

			It("Should contain string to text change in diff schema", func() {
				var excludedSchema warehouseutils.SchemaT
				currentSchema := warehouseutils.SchemaT{
					tableName: {
						"id":        "int",
						"record_id": "string",
						"sent_at":   "datetime",
						"name":      "string",
						"timestamp": "datetime",
					},
				}

				diff := warehouseutils.TableSchemaDiffT{
					Exists:    true,
					ColumnMap: map[string]string{},
					UpdatedSchema: map[string]string{
						"id":        "int",
						"record_id": "string",
						"sent_at":   "datetime",
						"name":      "text",
						"timestamp": "datetime",
					},
					StringColumnsToBeAlteredToText: []string{"name"},
				}
				tableSchemaDiff := GetTableSchemaDiff(tableName, currentSchema, uploadSchema, excludedSchema)
				Expect(tableSchemaDiff).To(Equal(diff))
			})
		})

		Context("With Excluded schema", func() {
			excludedSchema := warehouseutils.SchemaT{
				tableName: {
					"name":      "text",
					"timestamp": "datetime",
				},
			}
			It("Should exclude excluded schema in diff for a new event", func() {
				var currentSchema warehouseutils.SchemaT
				diff := warehouseutils.TableSchemaDiffT{
					Exists:           true,
					TableToBeCreated: true,
					ColumnMap: map[string]string{
						"id":        "int",
						"record_id": "string",
						"sent_at":   "datetime",
					},
					UpdatedSchema: map[string]string{
						"id":        "int",
						"record_id": "string",
						"sent_at":   "datetime",
					},
				}
				tableSchemaDiff := GetTableSchemaDiff(tableName, currentSchema, uploadSchema, excludedSchema)
				Expect(tableSchemaDiff).To(Equal(diff))
			})

			It("Should exclude excluded schema in diff for an existing event", func() {
				var excludedSchema warehouseutils.SchemaT
				currentSchema := warehouseutils.SchemaT{
					tableName: {
						"id":        "int",
						"record_id": "string",
					},
				}
				diff := warehouseutils.TableSchemaDiffT{
					Exists: true,
					ColumnMap: map[string]string{
						"sent_at": "datetime",
					},
					UpdatedSchema: map[string]string{
						"id":        "int",
						"record_id": "string",
						"sent_at":   "datetime",
					},
				}

				tableSchemaDiff := GetTableSchemaDiff(tableName, currentSchema, uploadSchema, excludedSchema)
				Expect(tableSchemaDiff).To(Equal(diff))
			})
		})
	})
})
