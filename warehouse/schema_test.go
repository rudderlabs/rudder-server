package warehouse_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/services/stats"
	. "github.com/rudderlabs/rudder-server/warehouse"
)

var _ = Describe("Schema", func() {
	stats.Setup()
	Describe("Handle schema change", func() {
		Context("No discards", func() {
			It("should send int values if existing datatype is int", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				var ok bool

				columnVal = 1.501
				convertedVal = 1
				newColumnVal, ok = HandleSchemaChange("int", "float", columnVal, "testDest")
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())
			})

			It("should send float values if existing datatype is float", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				var ok bool

				columnVal = 1
				convertedVal = 1.0
				newColumnVal, ok = HandleSchemaChange("float", "int", columnVal, "testDest")
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())
			})

			It("should send string values if existing datatype is string", func() {
				var newColumnVal, columnVal, convertedVal interface{}
				var ok bool

				columnVal = false
				convertedVal = "false"
				newColumnVal, ok = HandleSchemaChange("string", "boolean", columnVal, "testDest")
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = 1
				convertedVal = "1"
				newColumnVal, ok = HandleSchemaChange("string", "int", columnVal, "testDest")
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = 1.501
				convertedVal = "1.501"
				newColumnVal, ok = HandleSchemaChange("string", "float", columnVal, "testDest")
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())

				columnVal = "2022-05-05T00:00:00.000Z"
				convertedVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, ok = HandleSchemaChange("string", "datetime", columnVal, "testDest")
				Expect(newColumnVal).To(Equal(convertedVal))
				Expect(ok).To(BeTrue())
			})
		})

		Context("Discards", func() {
			It("existing datatype is boolean", func() {
				var newColumnVal, columnVal interface{}
				var ok bool

				columnVal = 1
				newColumnVal, ok = HandleSchemaChange("boolean", "int", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = 1.501
				newColumnVal, ok = HandleSchemaChange("boolean", "float", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "string value"
				newColumnVal, ok = HandleSchemaChange("boolean", "string", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, ok = HandleSchemaChange("boolean", "datetime", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())
			})

			It("existing datatype is int", func() {
				var newColumnVal, columnVal interface{}
				var ok bool

				columnVal = false
				newColumnVal, ok = HandleSchemaChange("int", "boolean", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "string value"
				newColumnVal, ok = HandleSchemaChange("int", "string", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, ok = HandleSchemaChange("int", "datetime", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())
			})

			It("existing datatype is float", func() {
				var newColumnVal, columnVal interface{}
				var ok bool

				columnVal = false
				newColumnVal, ok = HandleSchemaChange("float", "boolean", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "string value"
				newColumnVal, ok = HandleSchemaChange("float", "string", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "2022-05-05T00:00:00.000Z"
				newColumnVal, ok = HandleSchemaChange("float", "datetime", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())
			})

			It("existing datatype is datetime", func() {
				var newColumnVal, columnVal interface{}
				var ok bool
				columnVal = false
				newColumnVal, ok = HandleSchemaChange("datetime", "boolean", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = "string value"
				newColumnVal, ok = HandleSchemaChange("datetime", "string", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = 1
				newColumnVal, ok = HandleSchemaChange("datetime", "int", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())

				columnVal = 1.501
				newColumnVal, ok = HandleSchemaChange("datetime", "float", columnVal, "testDest")
				Expect(newColumnVal).To(BeNil())
				Expect(ok).To(BeFalse())
			})
		})
	})
})
