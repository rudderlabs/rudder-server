//go:build !warehouse_integration

package warehouse

import (
	"context"
	"net/http"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"

	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/protobuf/types/known/emptypb"
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
				pgResource *destination.PostgresResource
				err        error
				cleanup    = &testhelper.Cleanup{}
				w          *warehouseGRPC
				c          context.Context
				limit      = int32(2)
				uploadID   = int64(1)
			)

			BeforeAll(func() {
				pool, err := dockertest.NewPool("")
				Expect(err).To(BeNil())

				pgResource = setupWarehouseJobs(pool, GinkgoT(), cleanup)

				initWarehouse()

				err = setupDB(context.TODO(), getConnectionString())
				Expect(err).To(BeNil())

				sqlStatement, err := os.ReadFile("testdata/sql/4.sql")
				Expect(err).To(BeNil())

				_, err = pgResource.DB.Exec(string(sqlStatement))
				Expect(err).To(BeNil())

				pkgLogger = logger.NOP
			})

			BeforeEach(func() {
				sourceIDsByWorkspace = map[string][]string{
					workspaceID: {sourceID},
				}
				connectionsMap = map[string]map[string]warehouseutils.WarehouseT{
					destinationID: {
						sourceID: warehouseutils.WarehouseT{
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
		})

		Describe("Multi-tenant workspace", Ordered, func() {
			var (
				pgResource *destination.PostgresResource
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

				pgResource = setupWarehouseJobs(pool, g, cleanup)

				initWarehouse()

				err = setupDB(context.TODO(), getConnectionString())
				Expect(err).To(BeNil())

				sqlStatement, err := os.ReadFile("testdata/sql/4.sql")
				Expect(err).To(BeNil())

				_, err = pgResource.DB.Exec(string(sqlStatement))
				Expect(err).To(BeNil())

				pkgLogger = logger.NOP
				sourceIDsByWorkspace = map[string][]string{
					workspaceID: {sourceID},
				}
				connectionsMap = map[string]map[string]warehouseutils.WarehouseT{
					destinationID: {
						sourceID: warehouseutils.WarehouseT{
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
})
