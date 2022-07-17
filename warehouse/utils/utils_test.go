package warehouseutils_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	. "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestWarehouseT_GetBoolConfig(t *testing.T) {
	inputs := []struct {
		warehouse WarehouseT
		expected  bool
	}{
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{},
				},
			},
			expected: false,
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": "true",
					},
				},
			},
			expected: false,
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": false,
					},
				},
			},
			expected: false,
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": true,
					},
				},
			},
			expected: true,
		},
	}
	for idx, input := range inputs {
		got := input.warehouse.GetBoolDestinationConfig("k1")
		want := input.expected
		if got != want {
			t.Errorf("got %t expected %t input %d", got, want, idx)
		}
	}
}

var _ = Describe("Utils", func() {
	Describe("Locations", func() {
		Describe("S3", func() {
			Context("GetS3Location", func() {
				It("should parse url and return location and region", func() {
					var region, location string

					location, region = GetS3Location("https://test-bucket.s3.amazonaws.com/test-object.csv")
					Expect(region).To(Equal(""))
					Expect(location).To(Equal("s3://test-bucket/test-object.csv"))

					location, region = GetS3Location("https://test-bucket.s3.any-region.amazonaws.com/test-object.csv")
					Expect(region).To(Equal("any-region"))
					Expect(location).To(Equal("s3://test-bucket/test-object.csv"))

					location, region = GetS3Location("https://my.test-bucket.s3.amazonaws.com/test-object.csv")
					Expect(region).To(Equal(""))
					Expect(location).To(Equal("s3://my.test-bucket/test-object.csv"))

					location, region = GetS3Location("https://my.test-bucket.s3.us-west-1.amazonaws.com/test-object.csv")
					Expect(region).To(Equal("us-west-1"))
					Expect(location).To(Equal("s3://my.test-bucket/test-object.csv"))
				})
			})

			Context("GetS3LocationFolder", func() {
				It("should parse url and return location folder", func() {
					var location string

					location = GetS3LocationFolder("https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv")
					Expect(location).To(Equal("s3://test-bucket/myfolder"))

					location = GetS3LocationFolder("https://test-bucket.s3.eu-west-2.amazonaws.com/myfolder/test-object.csv")
					Expect(location).To(Equal("s3://test-bucket/myfolder"))

					location = GetS3LocationFolder("https://my.test-bucket.s3.eu-west-2.amazonaws.com/myfolder/test-object.csv")
					Expect(location).To(Equal("s3://my.test-bucket/myfolder"))
				})
			})

			Context("GetS3Locations", func() {
				It("should parse multiple urls and return array with locations ", func() {
					inputs := []LoadFileT{
						{Location: "https://test-bucket.s3.amazonaws.com/test-object.csv"},
						{Location: "https://test-bucket.s3.eu-west-1.amazonaws.com/test-object.csv"},
						{Location: "https://my.test-bucket.s3.amazonaws.com/test-object.csv"},
						{Location: "https://my.test-bucket.s3.us-west-1.amazonaws.com/test-object.csv"},
					}

					locations := GetS3Locations(inputs)
					Expect(locations[0].Location).To(Equal("s3://test-bucket/test-object.csv"))
					Expect(locations[1].Location).To(Equal("s3://test-bucket/test-object.csv"))
					Expect(locations[2].Location).To(Equal("s3://my.test-bucket/test-object.csv"))
					Expect(locations[3].Location).To(Equal("s3://my.test-bucket/test-object.csv"))
				})
			})
		})

		Describe("GCS", func() {
			Context("GetGCSLocation", func() {
				It("should parse url and return location", func() {
					var location string

					location = GetGCSLocation("https://storage.googleapis.com/test-bucket/test-object.csv", GCSLocationOptionsT{})
					Expect(location).To(Equal("gs://test-bucket/test-object.csv"))

					location = GetGCSLocation("https://storage.googleapis.com/my.test-bucket/test-object.csv", GCSLocationOptionsT{})
					Expect(location).To(Equal("gs://my.test-bucket/test-object.csv"))
				})
			})

			Context("GetGCSLocationFolder", func() {
				It("should parse url and return location folder", func() {
					var location string

					location = GetGCSLocationFolder("https://storage.googleapis.com/test-bucket/test-object.csv", GCSLocationOptionsT{})
					Expect(location).To(Equal("gs://test-bucket"))

					location = GetGCSLocationFolder("https://storage.googleapis.com/my.test-bucket/test-object.csv", GCSLocationOptionsT{})
					Expect(location).To(Equal("gs://my.test-bucket"))
				})
			})

			Context("GetGCSLocations", func() {
				It("should parse multiple urls and return array with locations ", func() {
					inputs := []LoadFileT{
						{Location: "https://storage.googleapis.com/test-bucket/test-object.csv"},
						{Location: "https://storage.googleapis.com/my.test-bucket/test-object.csv"},
						{Location: "https://storage.googleapis.com/my.test-bucket2/test-object.csv"},
						{Location: "https://storage.googleapis.com/my.test-bucket/test-object2.csv"},
					}

					locations := GetGCSLocations(inputs, GCSLocationOptionsT{})
					Expect(locations[0]).To(Equal("gs://test-bucket/test-object.csv"))
					Expect(locations[1]).To(Equal("gs://my.test-bucket/test-object.csv"))
					Expect(locations[2]).To(Equal("gs://my.test-bucket2/test-object.csv"))
					Expect(locations[3]).To(Equal("gs://my.test-bucket/test-object2.csv"))
				})
			})
		})

		Describe("Azure", func() {
			Context("GetAzureBlobLocation", func() {
				It("should parse url and return location", func() {
					location := GetAzureBlobLocation("https://myproject.blob.core.windows.net/test-bucket/test-object.csv")
					Expect(location).To(Equal("azure://myproject.blob.core.windows.net/test-bucket/test-object.csv"))
				})
			})

			Context("GetAzureBlobLocationFolder", func() {
				It("should parse url and return location folder", func() {
					location := GetAzureBlobLocationFolder("https://myproject.blob.core.windows.net/test-bucket/myfolder/test-object.csv")
					Expect(location).To(Equal("azure://myproject.blob.core.windows.net/test-bucket/myfolder"))
				})
			})
		})
	})
	Describe("Test DoubleQuoteAndJoinByComma", func() {
		It("should correctly apply double quotes and join by Commna ", func() {
			values := []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7"}
			Expect(DoubleQuoteAndJoinByComma(values)).To(Equal(`"column1","column2","column3","column4","column5","column6","column7"`))
		})
	})

	Describe("Time window warehouse destinations", func() {
		It("should give time window format based on warehouse destination type", func() {
			warehouse := WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: make(map[string]interface{}),
				},
			}
			warehouse.Destination.Config["tableSuffix"] = "key=val"
			timeWindow := time.Date(2022, time.Month(8), 6, 14, 10, 30, 0, time.UTC)

			warehouse.Type = S3_DATALAKE
			Expect(GetLoadFilePrefix(timeWindow, warehouse)).To(Equal("2022/08/06/14"))

			warehouse.Type = AZURE_DATALAKE
			Expect(GetLoadFilePrefix(timeWindow, warehouse)).To(Equal("2022/08/06/14"))

			warehouse.Type = GCS_DATALAKE
			Expect(GetLoadFilePrefix(timeWindow, warehouse)).To(Equal("key=val/2022/08/06/14"))
			warehouse.Destination.Config["timeWindowLayout"] = "year=2006/month=01/day=02/hour=15"
			Expect(GetLoadFilePrefix(timeWindow, warehouse)).To(Equal("key=val/year=2022/month=08/day=06/hour=14"))
		})
	})

	// Describe("Compare Schemas", func() {
	// 	Context("GetSchemaDiff", func() {
	// 		var currentSchema map[string]map[string]string
	// 		BeforeEach(func() {
	// 			currentSchema = map[string]map[string]string{
	// 				"table_1": {
	// 					"col_1": "string",
	// 					"col_2": "int",
	// 					"col_3": "bool",
	// 				},
	// 				"table_2": {
	// 					"col_1": "string",
	// 					"col_2": "int",
	// 					"col_3": "bool",
	// 				},
	// 			}
	// 		})

	// 		It("Should add new columns to existing table", func() {
	// 			uploadSchema := map[string]map[string]string{
	// 				"table_1": {
	// 					"col_4": "float",
	// 				},
	// 			}
	// 			columnMap := currentSchema["table_1"]
	// 			columnMap["col_4"] = "float"
	// 			diff := GetSchemaDiff(currentSchema, uploadSchema)
	// 			Expect(diff.UpdatedSchema["table_1"]).To(Equal(columnMap))
	// 		})

	// 		It("Should add new tables to existing schema", func() {
	// 			uploadSchema := map[string]map[string]string{
	// 				"new_table": {
	// 					"col_1": "float",
	// 				},
	// 			}
	// 			newColumnMap := map[string]string{
	// 				"col_1": "float",
	// 			}
	// 			diff := GetSchemaDiff(currentSchema, uploadSchema)
	// 			Expect(diff.UpdatedSchema["new_table"]).To(Equal(newColumnMap))
	// 		})

	// 		It("Should not alter tables that are not being updated", func() {
	// 			uploadSchema := map[string]map[string]string{
	// 				"table_1": {
	// 					"col_4": "float",
	// 				},
	// 			}
	// 			columnMap := currentSchema["table_2"]
	// 			diff := GetSchemaDiff(currentSchema, uploadSchema)
	// 			Expect(diff.UpdatedSchema["table_2"]).To(Equal(columnMap))
	// 		})
	// 	})
	// })
})
