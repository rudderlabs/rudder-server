package warehouse

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

var _ = Describe("WarehouseGrpc", func() {
	var (
		sourceID        = "test-sourceID"
		destinationID   = "test-destinationID"
		workspaceID     = "test-workspaceID"
		destinationType = "POSTGRES"
	)

	Describe("Warehouse GRPC round trip", Ordered, func() {
		Describe("Dedicated workspace", Ordered, func() {
			var (
				pgResource    *resource.PostgresResource
				minioResource *destination.MINIOResource
				err           error
				cleanup       = &testhelper.Cleanup{}
				w             *warehouseGRPC
				c             context.Context
				limit         = int32(2)
				uploadID      = int64(1)
			)

			BeforeAll(func() {
				pool, err := dockertest.NewPool("")
				Expect(err).To(BeNil())

				pgResource = setupWarehouseJobs(pool, GinkgoT())

				minioResource, err = destination.SetupMINIO(pool, cleanup)
				Expect(err).To(BeNil())

				initWarehouse()

				err = setupDB(context.TODO(), getConnectionString())
				Expect(err).To(BeNil())

				sqlStatement, err := os.ReadFile("testdata/sql/grpc_test.sql")
				Expect(err).To(BeNil())

				_, err = pgResource.DB.Exec(string(sqlStatement))
				Expect(err).To(BeNil())

				pkgLogger = logger.NOP
			})

			BeforeEach(func() {
				sourceIDsByWorkspace = map[string][]string{
					workspaceID: {sourceID},
				}
				connectionsMap = map[string]map[string]model.Warehouse{
					destinationID: {
						sourceID: model.Warehouse{
							Identifier: warehouseutils.GetWarehouseIdentifier(destinationType, sourceID, destinationID),
						},
					},
				}

				w = &warehouseGRPC{}
				c = context.TODO()
			})

			AfterAll(func() {
				cleanup.Run()
			})

			It("Init warehouse api", func() {
				err = InitWarehouseAPI(pgResource.DB, logger.NOP)
				Expect(err).To(BeNil())
			})

			It("Getting health", func() {
				res, err := w.GetHealth(c, &emptypb.Empty{})
				Expect(err).To(BeNil())
				Expect(res.Value).To(BeTrue())
			})

			Describe("Getting warehouse upload", func() {
				var req *proto.WHUploadRequest

				BeforeEach(func() {
					req = &proto.WHUploadRequest{
						UploadId:    uploadID,
						WorkspaceId: workspaceID,
					}
				})

				It("Invalid upload ID", func() {
					req.UploadId = -1
					_, err := w.GetWHUpload(c, req)

					Expect(err).NotTo(BeNil())
				})
				It("Unauthorized source", func() {
					sourceIDsByWorkspace = map[string][]string{}
					_, err := w.GetWHUpload(c, req)

					Expect(err).NotTo(BeNil())
				})
				It("Successfully get uploads", func() {
					res, err := w.GetWHUpload(c, req)

					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Id).Should(Equal(uploadID))
					Expect(res.Tables).Should(HaveLen(1))
					Expect(res.Tables[0].UploadId).Should(Equal(uploadID))
				})
			})

			Describe("Getting warehouse uploads", func() {
				var req *proto.WHUploadsRequest

				BeforeEach(func() {
					req = &proto.WHUploadsRequest{
						WorkspaceId:     workspaceID,
						SourceId:        sourceID,
						DestinationId:   destinationID,
						DestinationType: destinationType,
						Limit:           limit,
					}
				})

				It("Unauthorized source", func() {
					sourceIDsByWorkspace = map[string][]string{}

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(0))
				})
				It("Waiting syncs", func() {
					req.Status = "waiting"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(1))
				})
				It("Succeeded syncs", func() {
					req.Status = "success"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(4))
				})
				It("Failed syncs", func() {
					req.Status = "failed"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(2))
				})
				It("Aborted syncs", func() {
					req.Status = "aborted"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(3))
				})
				It("No status", func() {
					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(2))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(4))
					Expect(res.Uploads[1].Id).Should(BeEquivalentTo(3))
				})
				It("With offset", func() {
					req.Offset = 3

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(1))
				})
			})

			Describe("Count warehouse uploads to retry", func() {
				var req *proto.RetryWHUploadsRequest

				BeforeEach(func() {
					req = &proto.RetryWHUploadsRequest{
						WorkspaceId:     workspaceID,
						SourceId:        sourceID,
						DestinationId:   destinationID,
						DestinationType: destinationType,
					}
				})

				It("Invalid request", func() {
					req = &proto.RetryWHUploadsRequest{}

					res, err := w.CountWHUploadsToRetry(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Invalid retry Interval and uploadIDs", func() {
					req.IntervalInHours = -1
					req.UploadIds = []int64{}

					res, err := w.CountWHUploadsToRetry(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Unauthorized source", func() {
					req.IntervalInHours = 12
					sourceIDsByWorkspace = map[string][]string{}

					res, err := w.CountWHUploadsToRetry(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Invalid source", func() {
					req.IntervalInHours = 12
					req.SourceId = "test-sourceID-1"

					res, err := w.CountWHUploadsToRetry(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Interval in hours", func() {
					req.IntervalInHours = 24

					res, err := w.CountWHUploadsToRetry(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusOK))
					Expect(res.Count).Should(BeEquivalentTo(1))
				})
				It("UploadIDS", func() {
					req.UploadIds = []int64{3}

					res, err := w.CountWHUploadsToRetry(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusOK))
					Expect(res.Count).Should(BeEquivalentTo(1))
				})
			})

			Describe("Retry warehouse uploads", func() {
				var req *proto.RetryWHUploadsRequest

				BeforeEach(func() {
					req = &proto.RetryWHUploadsRequest{
						WorkspaceId:     workspaceID,
						SourceId:        sourceID,
						DestinationId:   destinationID,
						DestinationType: destinationType,
					}
				})

				It("Invalid request", func() {
					req = &proto.RetryWHUploadsRequest{}

					res, err := w.RetryWHUploads(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Invalid retry Interval and uploadIDs", func() {
					req.IntervalInHours = -1
					req.UploadIds = []int64{}

					res, err := w.RetryWHUploads(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Unauthorized source", func() {
					req.IntervalInHours = 12
					sourceIDsByWorkspace = map[string][]string{}

					res, err := w.RetryWHUploads(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Invalid source", func() {
					req.IntervalInHours = 12
					req.SourceId = "test-sourceID-1"

					res, err := w.RetryWHUploads(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Interval in hours", func() {
					req.IntervalInHours = 12

					res, err := w.RetryWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusOK))
				})
				It("UploadIDS", func() {
					req.IntervalInHours = 0
					req.UploadIds = []int64{3}

					res, err := w.RetryWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusOK))
				})
			})

			Describe("Triggering warehouse upload", func() {
				var req *proto.WHUploadRequest

				BeforeEach(func() {
					req = &proto.WHUploadRequest{
						UploadId:    3,
						WorkspaceId: workspaceID,
					}
				})

				It("Unauthorized source", func() {
					sourceIDsByWorkspace = map[string][]string{}

					res, err := w.TriggerWHUpload(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Triggered successfully", func() {
					res, err := w.TriggerWHUpload(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Message).Should(Equal(TriggeredSuccessfully))
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusOK))
				})
			})

			Describe("Triggering warehouse uploads", func() {
				var req *proto.WHUploadsRequest

				BeforeEach(func() {
					req = &proto.WHUploadsRequest{
						WorkspaceId:   workspaceID,
						SourceId:      sourceID,
						DestinationId: destinationID,
					}
				})

				It("Unauthorized source", func() {
					sourceIDsByWorkspace = map[string][]string{}

					res, err := w.TriggerWHUploads(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("Unknown destination", func() {
					req.DestinationId = ""

					res, err := w.TriggerWHUploads(c, req)
					Expect(err).NotTo(BeNil())
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusBadRequest))
				})
				It("No pending events", func() {
					req.DestinationId = "test-destinationID-1"
					res, err := w.TriggerWHUploads(c, req)

					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Message).Should(BeEquivalentTo(NoPendingEvents))
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusOK))
				})
				It("Triggered successfully", func() {
					res, err := w.TriggerWHUploads(c, req)

					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Message).Should(BeEquivalentTo(TriggeredSuccessfully))
					Expect(res.StatusCode).Should(BeEquivalentTo(http.StatusOK))
				})
			})

			Describe("Configuration validations", func() {
				var req *proto.WHValidationRequest

				BeforeEach(func() {
					req = &proto.WHValidationRequest{
						Path: "validate",
						Body: fmt.Sprintf(`{
								"destination": {
									"config": {
										"host":             %q,
										"database":         %q,
										"user":             %q,
										"password":         %q,
										"port":             %q,
										"sslMode":          "disable",
										"namespace":        "",
										"bucketProvider":   "MINIO",
										"bucketName":       %q,
										"accessKeyID":      %q,
										"secretAccessKey":  %q,
										"useSSL":           false,
										"endPoint":         %q,
										"syncFrequency":    "30",
										"useRudderStorage": false
									},
									"destinationDefinition": {
										"id":          "1bJ4YC7INdkvBTzotNh0zta5jDm",
										"name":        "POSTGRES",
										"displayName": "Postgres"
									}
								}
						}`,
							pgResource.Host,
							pgResource.Database,
							pgResource.User,
							pgResource.Password,
							pgResource.Port,
							minioResource.BucketName,
							minioResource.AccessKey,
							minioResource.SecretKey,
							minioResource.Endpoint,
						),
					}
				})

				When("Validating with steps", func() {
					DescribeTable("Validate", func(stepID, stepName string) {
						req.Step = stepID

						res, err := w.Validate(c, req)

						Expect(err).To(BeNil())
						Expect(res.Error).To(BeEmpty())
						Expect(res.Data).To(MatchJSON(fmt.Sprintf(`{
							  "success": true,
							  "error": "",
							  "steps": [
								{
								  "id": %s,
								  "name": %q,
								  "success": true,
								  "error": ""
								}
							  ]
							}`,
							stepID,
							stepName,
						)))
					},
						Entry("Verifying Object Storage", "1", "Verifying Object Storage"),
						Entry("Verifying Connections", "2", "Verifying Connections"),
						Entry("Verifying Create Schema", "3", "Verifying Create Schema"),
						Entry("Verifying Create and Alter Table", "4", "Verifying Create and Alter Table"),
						Entry("Verifying Fetch Schema", "5", "Verifying Fetch Schema"),
						Entry("Load Table", "6", "Verifying Load Table"),
					)
				})

				When("Validating without steps", func() {
					It("Validate", func() {
						res, err := w.Validate(c, req)
						Expect(err).To(BeNil())
						Expect(res.Error).To(BeEmpty())
						Expect(res.Data).To(MatchJSON(`
											{
											  "success": true,
											  "error": "",
											  "steps": [
												{
												  "id": 1,
												  "name": "Verifying Object Storage",
												  "success": true,
												  "error": ""
												},
												{
												  "id": 2,
												  "name": "Verifying Connections",
												  "success": true,
												  "error": ""
												},
												{
												  "id": 3,
												  "name": "Verifying Create Schema",
												  "success": true,
												  "error": ""
												},
												{
												  "id": 4,
												  "name": "Verifying Create and Alter Table",
												  "success": true,
												  "error": ""
												},
												{
												  "id": 5,
												  "name": "Verifying Fetch Schema",
												  "success": true,
												  "error": ""
												},
												{
												  "id": 6,
												  "name": "Verifying Load Table",
												  "success": true,
												  "error": ""
												}
											  ]
											}
										`))
					})
				})
			})
		})

		Describe("Multi-tenant workspace", Ordered, func() {
			var (
				pgResource *resource.PostgresResource
				err        error
				cleanup    = &testhelper.Cleanup{}
				w          *warehouseGRPC
				c          context.Context
				limit      = int32(2)
				g          = GinkgoT()
			)

			BeforeAll(func() {
				pool, err := dockertest.NewPool("")
				Expect(err).To(BeNil())

				g.Setenv("DEPLOYMENT_TYPE", "MULTITENANT")
				g.Setenv("HOSTED_SERVICE_SECRET", "test-secret")

				pgResource = setupWarehouseJobs(pool, g)

				initWarehouse()

				err = setupDB(context.TODO(), getConnectionString())
				Expect(err).To(BeNil())

				sqlStatement, err := os.ReadFile("testdata/sql/grpc_test.sql")
				Expect(err).To(BeNil())

				_, err = pgResource.DB.Exec(string(sqlStatement))
				Expect(err).To(BeNil())

				pkgLogger = logger.NOP
				sourceIDsByWorkspace = map[string][]string{
					workspaceID: {sourceID},
				}
				connectionsMap = map[string]map[string]model.Warehouse{
					destinationID: {
						sourceID: model.Warehouse{
							Identifier: warehouseutils.GetWarehouseIdentifier(destinationType, sourceID, destinationID),
						},
					},
				}

				w = &warehouseGRPC{}
				c = context.TODO()
			})

			AfterAll(func() {
				g.Setenv("DEPLOYMENT_TYPE", "")
				g.Setenv("HOSTED_SERVICE_SECRET", "")

				cleanup.Run()
			})

			It("Init warehouse api", func() {
				err = InitWarehouseAPI(pgResource.DB, logger.NOP)
				Expect(err).To(BeNil())
			})

			Describe("Getting warehouse uploads", func() {
				var req *proto.WHUploadsRequest

				BeforeEach(func() {
					req = &proto.WHUploadsRequest{
						WorkspaceId:     workspaceID,
						SourceId:        sourceID,
						DestinationId:   destinationID,
						DestinationType: destinationType,
						Limit:           limit,
					}
				})

				It("Waiting syncs", func() {
					req.Status = "waiting"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(1))
				})
				It("Succeeded syncs", func() {
					req.Status = "success"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(4))
				})
				It("Failed syncs", func() {
					req.Status = "failed"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(2))
				})
				It("Aborted syncs", func() {
					req.Status = "aborted"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(3))
				})
				It("No status", func() {
					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(2))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(4))
					Expect(res.Uploads[1].Id).Should(BeEquivalentTo(3))
				})
				It("With offset", func() {
					req.Offset = 3

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).Should(BeEquivalentTo(1))
				})
			})
		})
	})
	Describe("Test for validation of  proto message for  object storage destination validation", func() {
		Context("Handling error cases", func() {
			It("should throw Code_INVALID_ARGUMENT when received unsupported/invalid object storage destination type", func() {
				_, err := validateObjectStorageRequestBody(&proto.ValidateObjectStorageRequest{Type: "ABC"})
				statusError, _ := status.FromError(err)
				Expect(statusError.Err().Error()).To(BeIdenticalTo("rpc error: code = InvalidArgument desc = invalid argument err: \ntype: ABC not supported"))
				Expect(statusError.Code()).To(BeIdenticalTo(codes.Code(code.Code_INVALID_ARGUMENT)))
			})
			It("should throw Code_INVALID_ARGUMENT when received no bucketName(Not applicable for AZURE_BLOB)", func() {
				configMap := &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"containerName": {
							Kind: &structpb.Value_StringValue{
								StringValue: "tempContainerName",
							},
						},
					},
				}
				_, err := validateObjectStorageRequestBody(&proto.ValidateObjectStorageRequest{Type: "S3", Config: configMap})
				statusError, _ := status.FromError(err)
				Expect(statusError.Code()).To(BeIdenticalTo(codes.Code(code.Code_INVALID_ARGUMENT)))
			})
			It("should throw Code_INVALID_ARGUMENT when received no containerName(For AZURE_BLOB only)", func() {
				configMap := &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"bucketName": {
							Kind: &structpb.Value_StringValue{
								StringValue: "tempbucket",
							},
						},
					},
				}
				_, err := validateObjectStorageRequestBody(&proto.ValidateObjectStorageRequest{Type: "AZURE_BLOB", Config: configMap})
				statusError, _ := status.FromError(err)
				Expect(statusError.Code()).To(BeIdenticalTo(codes.Code(code.Code_INVALID_ARGUMENT)))
				Expect(statusError.Err().Error()).To(BeIdenticalTo("rpc error: code = InvalidArgument desc = invalid argument err: \ncontainerName invalid or not present"))
			})
		})
	})
})
