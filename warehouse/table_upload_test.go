//go:build !warehouse_integration

package warehouse

import (
	"context"
	"errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func tableUploadsSQLStatement() string {
	return `
			BEGIN;
			INSERT INTO wh_load_files (
			  id, staging_file_id, location, source_id,
			  destination_id, destination_type,
			  table_name, total_events, created_at,
			  metadata
			)
			VALUES
			  (
				1, 1, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'test-sourceID', 'test-destinationID',
				'POSTGRES', 'test-table', 1, NOW(),
				'{}'
			  ),
			  (
				2, 2, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'test-sourceID', 'test-destinationID',
				'POSTGRES', 'test-table', 1, NOW(),
				'{}'
			  ),
			  (
				3, 3, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'test-sourceID', 'test-destinationID',
				'POSTGRES', 'test-table', 1, NOW(),
				'{}'
			  ),
			  (
				4, 4, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'test-sourceID', 'test-destinationID',
				'POSTGRES', 'test-table', 1, NOW(),
				'{}'
			  ),
			  (
				5, 1, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'test-sourceID', 'test-destinationID',
				'POSTGRES', 'rudder-discards', 1, NOW(),
				'{}'
			  );
			END;
	`
}

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

			pgResource = setupWarehouseJobs(pool, GinkgoT(), cleanup)

			initWarehouse()

			err = setupDB(context.TODO(), getConnectionString())
			Expect(err).To(BeNil())

			pkgLogger = &logger.NOP{}
		})

		AfterAll(func() {
			cleanup.Run()
		})

		It("Setup table uploads", func() {
			_, err = pgResource.DB.Exec(tableUploadsSQLStatement())
			Expect(err).To(BeNil())
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
					upload: &UploadT{
						ID: uploadID,
					},
					dbHandle: pgResource.DB,
				})
				Expect(err).To(BeNil())
			})

			It("Check table upload count", func() {
				count, err := tu.getTotalEvents()
				Expect(err).To(BeNil())
				Expect(count).To(BeEquivalentTo(4))
			})

			It("Getting number of events", func() {
				count, err := tu.getNumEvents()
				Expect(err).To(BeNil())
				Expect(count).To(BeEquivalentTo(4))
			})

			It("Getting number of events", func() {
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
						upload: &UploadT{
							ID: uploadID,
						},
						warehouse: warehouseutils.WarehouseT{
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
