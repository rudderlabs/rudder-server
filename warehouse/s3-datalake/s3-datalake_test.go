package s3datalake_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	s3datalake "github.com/rudderlabs/rudder-server/warehouse/s3-datalake"
)

var _ = Describe("S3Datalake", func() {
	var (
		sd *s3datalake.HandleT
	)

	BeforeEach(func() {
		sd = &s3datalake.HandleT{}
		sd.Warehouse.Destination.Config = map[string]interface{}{
			"region":      "us-east-1",
			"accessKeyID": "AKIAWTVBJHCTKWPYEFJ3",
			"accessKey":   "fcgOL1JOjTYWlQY6YE30ninmPnuHqnLSditk4vC+",
		}
	})

	Context("AWS Glue", func() {
		It("should create a new glue client", func() {

			_, err := sd.SgetGlueClient()
			Expect(err).To(BeNil())
		})

		FIt("should fetch schema", func() {
			sd.Warehouse.Destination.Config["database"] = "ruddertestdb"

			s, e := sd.FetchSchema(sd.Warehouse)
			fmt.Println(s, e)
		})

		It("should create a table in glue", func() {
			glueClient, err := sd.SgetGlueClient()
			Expect(err).To(BeNil())

			out, err := glueClient.GetTables(&glue.GetTablesInput{
				DatabaseName: aws.String("redshift-glue-db-test"),
			})

			fmt.Println("len:", len(out.TableList))
			fmt.Println("nextToken: ", out.NextToken, *out.NextToken)

			// out, err := glueClient.CreateTable(&glue.CreateTableInput{
			// 	DatabaseName: aws.String("ruddertestdb"),
			// 	TableInput: &glue.TableInput{
			// 		Name: aws.String("newTable1"),
			// 		StorageDescriptor: &glue.StorageDescriptor{
			// 			Columns: []*glue.Column{
			// 				{
			// 					Name: aws.String("name"),
			// 					Type: aws.String(glue.ColumnStatisticsTypeString),
			// 				},
			// 				{
			// 					Name: aws.String("text"),
			// 					Type: aws.String(glue.ColumnStatisticsTypeString),
			// 				},
			// 			},
			// 			Location: aws.String("s3://dhawal-data-lake-test/datalake"),
			// 		},
			// 	},
			// })

			// fmt.Println(out, err)
		})
	})
})
