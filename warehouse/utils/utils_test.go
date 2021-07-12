package warehouseutils_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/warehouse"
	. "github.com/rudderlabs/rudder-server/warehouse/utils"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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
					var inputs = []string{
						"https://test-bucket.s3.amazonaws.com/test-object.csv",
						"https://test-bucket.s3.eu-west-1.amazonaws.com/test-object.csv",
						"https://my.test-bucket.s3.amazonaws.com/test-object.csv",
						"https://my.test-bucket.s3.us-west-1.amazonaws.com/test-object.csv",
					}

					locations := GetS3Locations(inputs)
					Expect(locations[0]).To(Equal("s3://test-bucket/test-object.csv"))
					Expect(locations[1]).To(Equal("s3://test-bucket/test-object.csv"))
					Expect(locations[2]).To(Equal("s3://my.test-bucket/test-object.csv"))
					Expect(locations[3]).To(Equal("s3://my.test-bucket/test-object.csv"))
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
					var inputs = []string{
						"https://storage.googleapis.com/test-bucket/test-object.csv",
						"https://storage.googleapis.com/my.test-bucket/test-object.csv",
						"https://storage.googleapis.com/my.test-bucket2/test-object.csv",
						"https://storage.googleapis.com/my.test-bucket/test-object2.csv",
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

	FIt("parquet", func() {

		// file, err := os.OpenFile("/Users/dhawal/rudder-server/parquetTest/c1.parquet", os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0660)
		// if err != nil {
		// 	return
		// }
		// w := bufio.NewWriter(file)
		// fmt.Println(fmt.Sprintf(err)
		// Expect(err).To(BeNil())

		// pqloader := &ParquetLoader{
		// 	FileWriter: w,
		// }
		// fmt.Println(fmt.Sprintf("lullll", pqloader)

		// pqloader.AddColumn("Abc", "ABC")
		// pqloader.AddColumn("Abcd", "ABCD")
		// pqloader.AddColumn("Num", "dada")
		// pqloader.AddColumn("Num1111", "111dada")
		// pqloader.AddEmptyColumn("empty")

		// err = pqloader.WriteEventToFile()
		// fmt.Println(fmt.Sprintf(err)

		// err = w.Flush()
		// Expect(err).To(BeNil())
		// err = file.Close()
		// Expect(err).To(BeNil())
		// fmt.Println(fmt.Sprintf(err)

		j := warehouse.CreateDummyJobRun()

		w, err := j.GetWriter("t1")
		Expect(err).To(BeNil())

		i := 0
		for i = 0; i < 10; i++ {
			ploader := NewParquetLoader("RS", w)
			ploader.AddColumn("rsbigint", "bigint", int64(i))
			ploader.AddColumn("rsbool", "bool", i%2 == 0)
			ploader.AddColumn("rsdatetime", "datetime", time.Now().Add(time.Duration(i)*time.Second).UnixNano()/int64(time.Microsecond))
			ploader.AddColumn("rsdouble", "double", float64(0.1112+float64(i)))
			ploader.AddColumn("rsstring", "string", fmt.Sprintf("somestr%d", i))
			// ploader.AddColumn("rstext", fmt.Sprintf("sometxt%d", i))
			ploader.AddColumn("rstext", "text", nil)

			err = ploader.Write()
			fmt.Println(err)
			Expect(err).To(BeNil())
		}
		err = w.Close()
		fmt.Println("close err", err)
		Expect(err).To(BeNil())

	})

	It("get parquet type", func() {
		var pVal interface{}
		var err error

		pVal, err = warehouseutils.GetParquetValue("true", "boolean")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, "true"))
		pVal, err = warehouseutils.GetParquetValue("false", "boolean")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, "false"))
		pVal, err = warehouseutils.GetParquetValue("True", "boolean")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, "True"))
		pVal, err = warehouseutils.GetParquetValue("False", "boolean")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, "False"))
		pVal, err = warehouseutils.GetParquetValue("1112", "int")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, "1112"))
		pVal, err = warehouseutils.GetParquetValue(1112.1112, "int")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, 1112.1112))
		pVal, err = warehouseutils.GetParquetValue("1112", "bigint")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, "1112"))
		pVal, err = warehouseutils.GetParquetValue(1112.1112, "float")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, 1112.1112))
		pVal, err = warehouseutils.GetParquetValue("abcddd", "string")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, "abcddd"))
		pVal, err = warehouseutils.GetParquetValue(1234, "string")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, 1234))
		pVal, err = warehouseutils.GetParquetValue("abcddd", "text")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, "abcddd"))
		pVal, err = warehouseutils.GetParquetValue(1234, "text")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, 1234))
		pVal, err = warehouseutils.GetParquetValue(time.Now().Format(time.RFC3339), "datetime")
		Expect(err).To(BeNil())
		fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, time.Now().UnixNano()/int64(time.Millisecond)))
		// pVal, err = warehouseutils.GetParquetValue("2009-05-19 14:39:22", "datetime")
		// Expect(err).To(BeNil())
		// fmt.Println(fmt.Sprintf("parquet val : %[1]v type : %[1]T expected: %[2]v", pVal, 1111))
	})
})

// type w struct {
// }

// func (w w) Write(p []byte) (int, error) {
// 	fmt.Println(fmt.Sprintf(string(p))
// 	return len(p), nil
// }
