package warehouse_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/rudderlabs/rudder-server/warehouse"
)

var _ = Describe("Schema", func() {
	Describe("Handle schema change", func() {
		Context("No discards", func() {
			It("should send int values if existing datatype is int", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				columnVal = 1.501
				convertedVal = 1
				newColumnVal, _ = HandleSchemaChange("int", "float", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
			})

			It("should send float values if existing datatype is float", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				columnVal = 1
				convertedVal = 1.0
				newColumnVal, _ = HandleSchemaChange("float", "int", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
			})

			It("should send string values if existing datatype is string", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				columnVal = false
				convertedVal = "false"
				newColumnVal, _ = HandleSchemaChange("string", "boolean", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				columnVal = 1
				convertedVal = "1"
				newColumnVal, _ = HandleSchemaChange("string", "int", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				columnVal = 1.501
				convertedVal = "1.501"
				newColumnVal, _ = HandleSchemaChange("string", "float", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				columnVal = "2022-05-05T00:00:00.000Z"
				convertedVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, _ = HandleSchemaChange("string", "datetime", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				columnVal = `{"json":true}`
				convertedVal = `{"json":true}`
				newColumnVal, _ = HandleSchemaChange("string", "json", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))
			})

			It("should send json string values if existing datatype is json", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				columnVal = false
				convertedVal = "false"
				newColumnVal, _ = HandleSchemaChange("json", "boolean", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				columnVal = 1
				convertedVal = "1"
				newColumnVal, _ = HandleSchemaChange("json", "int", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				columnVal = 1.501
				convertedVal = "1.501"
				newColumnVal, _ = HandleSchemaChange("json", "float", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				columnVal = "2022-05-05T00:00:00.000Z"
				convertedVal = "\"2022-05-05T00:00:00.000Z\""
				newColumnVal, _ = HandleSchemaChange("json", "datetime", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				columnVal = "string value"
				convertedVal = `"string value"`
				newColumnVal, _ = HandleSchemaChange("json", "string", columnVal)
				Expect(newColumnVal).To(Equal(convertedVal))

				var columnArrVal []interface{}
				columnArrVal = append(columnArrVal, false, 1, "string value")
				newColumnVal, _ = HandleSchemaChange("json", "string", columnArrVal)
				Expect(newColumnVal).To(Equal(columnArrVal))
			})
		})

		Context("Discards", func() {
			It("existing datatype is boolean", func() {
				var newColumnVal, columnVal interface{}
				columnVal = 1
				newColumnVal, _ = HandleSchemaChange("boolean", "int", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = 1.501
				newColumnVal, _ = HandleSchemaChange("boolean", "float", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = "string value"
				newColumnVal, _ = HandleSchemaChange("boolean", "string", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, _ = HandleSchemaChange("boolean", "datetime", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = `{"json":true}`
				newColumnVal, _ = HandleSchemaChange("boolean", "json", columnVal)
				Expect(newColumnVal).To(BeNil())
			})

			It("existing datatype is int", func() {
				var newColumnVal, columnVal interface{}
				columnVal = false
				newColumnVal, _ = HandleSchemaChange("int", "boolean", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = "string value"
				newColumnVal, _ = HandleSchemaChange("int", "string", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, _ = HandleSchemaChange("int", "datetime", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = `{"json":true}`
				newColumnVal, _ = HandleSchemaChange("int", "json", columnVal)
				Expect(newColumnVal).To(BeNil())
			})

			It("existing datatype is float", func() {
				var newColumnVal, columnVal interface{}
				columnVal = false
				newColumnVal, _ = HandleSchemaChange("float", "boolean", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = "string value"
				newColumnVal, _ = HandleSchemaChange("float", "string", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, _ = HandleSchemaChange("float", "datetime", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = `{"json":true}`
				newColumnVal, _ = HandleSchemaChange("float", "json", columnVal)
				Expect(newColumnVal).To(BeNil())
			})

			It("existing datatype is datetime", func() {
				var newColumnVal, columnVal interface{}
				columnVal = false
				newColumnVal, _ = HandleSchemaChange("datetime", "boolean", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = "string value"
				newColumnVal, _ = HandleSchemaChange("datetime", "string", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = 1
				newColumnVal, _ = HandleSchemaChange("datetime", "int", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = 1.501
				newColumnVal, _ = HandleSchemaChange("datetime", "float", columnVal)
				Expect(newColumnVal).To(BeNil())

				columnVal = `{"json":true}`
				newColumnVal, _ = HandleSchemaChange("datetime", "json", columnVal)
				Expect(newColumnVal).To(BeNil())
			})
		})
	})
})
