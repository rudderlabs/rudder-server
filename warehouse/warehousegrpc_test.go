package warehouse

import (
	"context"
	"net/http"

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

func syncsSQLStatement() string {
	return `
			BEGIN;

			INSERT INTO wh_uploads (
			  id, source_id, namespace, destination_id,
			  destination_type, start_staging_file_id,
			  end_staging_file_id, start_load_file_id,
			  end_load_file_id, status, schema,
			  error, metadata, first_event_at,
			  last_event_at, created_at, updated_at
			)
			VALUES
			  (
				1, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				1, 1, 1, 1, 'waiting', '{}', '{}', '{}',
				now(), now(), now(), now()
			  ),
			  (
				2, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				1, 1, 1, 1, 'exporting_data_failed',
				'{}', '{}', '{}', now(), now(), now(),
				now()
			  ),
			  (
				3, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				1, 1, 1, 1, 'aborted', '{}', '{}', '{}',
				now(), now(), now(), now()
			  ),
			  (
				4, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				1, 1, 1, 1, 'exported_data', '{}', '{}',
				'{}', now(), now(), now(), now()
			  );

			INSERT INTO wh_table_uploads (
			  id, wh_upload_id, table_name, status,
			  error, created_at, updated_at
			)
			VALUES
			  (
				1, 1, 'test-table', 'waiting', '',
				NOW(), NOW()
			  ),
			  (
				2, 2, 'test-table', 'failed', '',
				NOW(), NOW()
			  ),
			  (
				3, 3, 'test-table', 'failed', '',
				NOW(), NOW()
			  ),
			  (
				4, 4, 'test-table', 'succeeded', '',
				NOW(), NOW()
			  );
			END;
	`
}

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
				w          *warehouseGrpc
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

				pkgLogger = &logger.NOP{}
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

				w = &warehouseGrpc{}
				c = context.TODO()
			})

			AfterAll(func() {
				cleanup.Run()
			})

			It("Init warehouse api", func() {
				err = InitWarehouseAPI(pgResource.DB, &logger.NOP{})
				Expect(err).To(BeNil())

				_, err = pgResource.DB.Exec(syncsSQLStatement())
				Expect(err).To(BeNil())
			})

			It("Getting health", func() {
				res, err := w.GetHealth(c, &emptypb.Empty{})
				Expect(err).To(BeNil())
				Expect(res.Value).To(BeTrue())
			})

			It("Getting warehouse upload", func() {
				res, err := w.GetWHUpload(c, &proto.WHUploadRequest{
					UploadId:    uploadID,
					WorkspaceId: workspaceID,
				})
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeNil())
				Expect(res.Id).To(Equal(uploadID))
				Expect(res.Tables).Should(HaveLen(1))
				Expect(res.Tables[0].UploadId).To(Equal(uploadID))
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
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(1))
				})
				It("Succeeded syncs", func() {
					req.Status = "success"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(4))
				})
				It("Failed syncs", func() {
					req.Status = "failed"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(2))
				})
				It("Aborted syncs", func() {
					req.Status = "aborted"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(3))
				})
				It("No status", func() {
					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(2))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(4))
					Expect(res.Uploads[1].Id).To(BeEquivalentTo(3))
				})
				It("With offset", func() {
					req.Offset = 3

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(1))
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

				It("Interval in hours", func() {
					req.IntervalInHours = 24

					res, err := w.CountWHUploadsToRetry(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.StatusCode).To(BeEquivalentTo(http.StatusOK))
					Expect(res.Count).To(BeEquivalentTo(1))
				})
				It("Interval in hours", func() {
					req.UploadIds = []int64{3}

					res, err := w.CountWHUploadsToRetry(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.StatusCode).To(BeEquivalentTo(http.StatusOK))
					Expect(res.Count).To(BeEquivalentTo(1))
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

				It("Interval in hours", func() {
					req.IntervalInHours = 24

					res, err := w.RetryWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.StatusCode).To(BeEquivalentTo(http.StatusOK))
				})
				It("Interval in hours", func() {
					req.UploadIds = []int64{3}

					res, err := w.RetryWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.StatusCode).To(BeEquivalentTo(http.StatusOK))
				})
			})

			It("Triggering warehouse upload", func() {
				res, err := w.TriggerWHUpload(c, &proto.WHUploadRequest{
					UploadId:    3,
					WorkspaceId: workspaceID,
				})
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeNil())
				Expect(res.Message).To(Equal(TriggeredSuccessfully))
				Expect(res.StatusCode).To(BeEquivalentTo(http.StatusOK))
			})

			It("Triggering warehouse uploads", func() {
				res, err := w.TriggerWHUploads(c, &proto.WHUploadsRequest{
					WorkspaceId:   workspaceID,
					SourceId:      sourceID,
					DestinationId: destinationID,
				})
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeNil())
				Expect(res.Message).To(Equal(TriggeredSuccessfully))
				Expect(res.StatusCode).To(BeEquivalentTo(http.StatusOK))
			})
		})

		Describe("Multi-tenant workspace", Ordered, func() {
			var (
				pgResource *destination.PostgresResource
				err        error
				cleanup    = &testhelper.Cleanup{}
				w          *warehouseGrpc
				c          context.Context
				limit      = int32(2)
				es         = GinkgoT()
			)

			BeforeAll(func() {
				pool, err := dockertest.NewPool("")
				Expect(err).To(BeNil())

				es.Setenv("DEPLOYMENT_TYPE", "MULTITENANT")
				es.Setenv("HOSTED_SERVICE_SECRET", "test-secret")

				pgResource = setupWarehouseJobs(pool, es, cleanup)

				initWarehouse()

				err = setupDB(context.TODO(), getConnectionString())
				Expect(err).To(BeNil())

				pkgLogger = &logger.NOP{}
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

				w = &warehouseGrpc{}
				c = context.TODO()
			})

			AfterAll(func() {
				es.Setenv("DEPLOYMENT_TYPE", "")
				es.Setenv("HOSTED_SERVICE_SECRET", "")

				cleanup.Run()
			})

			It("Init warehouse api", func() {
				err = InitWarehouseAPI(pgResource.DB, &logger.NOP{})
				Expect(err).To(BeNil())

				_, err = pgResource.DB.Exec(syncsSQLStatement())
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
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(1))
				})
				It("Succeeded syncs", func() {
					req.Status = "success"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(4))
				})
				It("Failed syncs", func() {
					req.Status = "failed"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(2))
				})
				It("Aborted syncs", func() {
					req.Status = "aborted"

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(3))
				})
				It("No status", func() {
					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(2))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(4))
					Expect(res.Uploads[1].Id).To(BeEquivalentTo(3))
				})
				It("With offset", func() {
					req.Offset = 3

					res, err := w.GetWHUploads(c, req)
					Expect(err).To(BeNil())
					Expect(res).NotTo(BeNil())
					Expect(res.Uploads).Should(HaveLen(1))
					Expect(res.Uploads[0].Id).To(BeEquivalentTo(1))
				})
			})
		})
	})
})
