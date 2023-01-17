package warehouse

import (
	"context"
	"errors"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ = Describe("TableUpload", func() {
	Describe("Tale uploads round trip", Ordered, func() {
		var (
			pgResource     *destination.PostgresResource
			err            error
			cleanup        = &testhelper.Cleanup{}
			uploadID       = int64(1)
			stagingFileIDs = []int64{1, 2, 3, 4, 5}
			tableNames     = []string{"test-table", "rudder-discards"}
		)

		BeforeAll(func() {
			pool, err := dockertest.NewPool("")
			Expect(err).To(BeNil())

			pgResource = setupWarehouseJobs(pool, GinkgoT())
			initWarehouse()

			err = setupDB(context.TODO(), getConnectionString())
			Expect(err).To(BeNil())

			sqlStatement, err := os.ReadFile("testdata/sql/table_uploads_test.sql")
			Expect(err).To(BeNil())

			_, err = pgResource.DB.Exec(string(sqlStatement))
			Expect(err).To(BeNil())

			pkgLogger = logger.NOP
		})

		AfterAll(func() {
			cleanup.Run()
		})

		It("Verify if no table uploads are created", func() {
			Expect(areTableUploadsCreated(uploadID)).To(BeFalse())
		})

		It("Create table uploads for batch", func() {
			err = createTableUploads(uploadID, tableNames)
			Expect(err).To(BeNil())
		})

		It("Verify if table uploads are created", func() {
			Expect(areTableUploadsCreated(uploadID)).To(BeTrue())
		})

		Describe("Operations for table uploads", func() {
			var tu *TableUploadT
			var tableName string

			BeforeEach(func() {
				tableName = "test-table"
				tu = NewTableUpload(uploadID, tableName)
			})

			It("Create table upload", func() {
				err = tu.updateTableEventsCount(&UploadJobT{
					stagingFileIDs: stagingFileIDs,
					upload: model.Upload{
						ID: uploadID,
					},
					dbHandle: pgResource.DB,
				})
				Expect(err).To(BeNil())
			})

			It("Getting total number of events", func() {
				count, err := tu.getTotalEvents()
				Expect(err).To(BeNil())
				Expect(count).To(BeEquivalentTo(4))
			})

			It("Getting number of events", func() {
				count, err := tu.getNumEvents()
				Expect(err).To(BeNil())
				Expect(count).To(BeEquivalentTo(4))
			})

			It("Setting error", func() {
				err := tu.setError("exporting_data_failed", errors.New("test error"))
				Expect(err).To(BeNil())
			})

			It("Setting status", func() {
				err := tu.setStatus("exported_data")
				Expect(err).To(BeNil())
			})

			Describe("Getting number of events", func() {
				var job *UploadJobT

				BeforeEach(func() {
					job = &UploadJobT{
						upload: model.Upload{
							ID: uploadID,
						},
						warehouse: warehouseutils.Warehouse{
							Type: "POSTGRES",
						},
					}
				})

				It("Getting number of events without discards", func() {
					count, err := job.getTotalEventsUploaded(false)
					Expect(err).To(BeNil())
					Expect(count).To(BeEquivalentTo(4))
				})

				It("Getting number of events with discards", func() {
					count, err := job.getTotalEventsUploaded(true)
					Expect(err).To(BeNil())
					Expect(count).To(BeEquivalentTo(4))
				})
			})
		})
	})
})
