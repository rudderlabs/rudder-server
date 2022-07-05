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
})
