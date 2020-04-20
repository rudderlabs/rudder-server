package warehouseutils_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/rudderlabs/rudder-server/warehouse/utils"
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
})
