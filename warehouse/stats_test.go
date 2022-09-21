//go:build !warehouse_integration

package warehouse

import (
	"context"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ory/dockertest/v3"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/services/stats"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func statsSQLStatement() string {
	return `
			BEGIN;
			INSERT INTO wh_staging_files (
			  id, location, schema, source_id, destination_id,
			  status, total_events, first_event_at,
			  last_event_at, created_at, updated_at,
			  metadata
			)
			VALUES
			  (
				1, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'{}', 'test-sourceID', 'test-destinationID',
				'succeeded', 1, NOW(), NOW(), NOW(),
				NOW(), '{}'
			  ),
			  (
				2, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'{}', 'test-sourceID', 'test-destinationID',
				'succeeded', 1, NOW(), NOW(), NOW(),
				NOW(), '{}'
			  ),
			  (
				3, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'{}', 'test-sourceID', 'test-destinationID',
				'succeeded', 1, NOW(), NOW(), NOW(),
				NOW(), '{}'
			  ),
			  (
				4, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'{}', 'test-sourceID', 'test-destinationID',
				'succeeded', 1, NOW(), NOW(), NOW(),
				NOW(), '{}'
			  );
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
			  );
			INSERT INTO wh_table_uploads (
			  id, wh_upload_id, table_name, status, total_events,
			  error, created_at, updated_at
			)
			VALUES
			  (
				1, 1, 'test-table-1', 'exported_data', 1, '',
				NOW(), NOW()
			  ),
			  (
				2, 1, 'test-table-2', 'exported_data', 1, '',
				NOW(), NOW()
			  ),
			  (
				3, 1, 'test-table-3', 'exported_data', 1, '',
				NOW(), NOW()
			  ),
			  (
				4, 1, 'test-table-4', 'exported_data', 1, '',
				NOW(), NOW()
			  );
			END;
	`
}

var _ = Describe("Stats", Ordered, func() {
	var (
		sourceID        = "test-sourceID"
		destinationID   = "test-destinationID"
		destinationType = "test-desinationType"
		destinationName = "test-destinationName"
		sourceName      = "test-sourceName"
		statName        = "test-statName"
		es              = GinkgoT()
		pgResource      *destination.PostgresResource
		err             error
		uploadID        = int64(1)
		cleanup         = &testhelper.Cleanup{}
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
		_, err = pgResource.DB.Exec(statsSQLStatement())
		Expect(err).To(BeNil())
	})

	Describe("Jobs stats", func() {
		BeforeEach(func() {
			ctrl := gomock.NewController(es)
			mockStats := mock_stats.NewMockStats(ctrl)
			mockRudderStats := mock_stats.NewMockRudderStats(ctrl)

			mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mockRudderStats)
			mockRudderStats.EXPECT().Count(gomock.Any()).AnyTimes()

			stats.DefaultStats = mockStats
		})

		Describe("Job run stats", func() {
			var jr *JobRunT

			BeforeEach(func() {
				jr = &JobRunT{
					job: PayloadT{
						DestinationID:   destinationID,
						SourceID:        sourceID,
						DestinationName: destinationName,
						SourceName:      sourceName,
					},
				}
			})

			It("timer stat", func() {
				jr.timerStat(statName)
			})

			It("counter stat", func() {
				jr.counterStat(statName)
			})
		})

		Describe("Upload job stats", func() {
			var u *UploadJobT

			BeforeEach(func() {
				u = &UploadJobT{
					warehouse: warehouseutils.WarehouseT{
						Type: "POSTGRES",
						Destination: backendconfig.DestinationT{
							ID:   destinationID,
							Name: destinationName,
						},
						Source: backendconfig.SourceT{
							ID:   destinationID,
							Name: destinationName,
						},
					},
					upload: &UploadT{
						DestinationID: destinationID,
						SourceID:      sourceID,
					},
				}
			})

			It("timer stat", func() {
				u.timerStat(statName)
			})

			It("counter stat", func() {
				u.counterStat(statName)
			})

			It("gauge stat", func() {
				u.guageStat(statName)
			})
		})

		It("record staged rows stat", func() {
			recordStagedRowsStat(100, destinationType, destinationID, sourceName, destinationName, sourceID)
		})

		It("upload status stat", func() {
			getUploadStatusStat(statName, destinationType, destinationID, sourceName, destinationName, sourceID)
		})

		It("persis ssl file error stat", func() {
			persistSSLFileErrorStat(destinationType, destinationName, destinationID, sourceName, sourceID, "")
		})
	})

	Describe("Generate upload success/aborted metrics", func() {
		var job *UploadJobT

		BeforeEach(func() {
			ctrl := gomock.NewController(es)
			mockStats := mock_stats.NewMockStats(ctrl)
			mockRudderStats := mock_stats.NewMockRudderStats(ctrl)

			mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(3).Return(mockRudderStats)
			mockRudderStats.EXPECT().Count(4).Times(2)
			mockRudderStats.EXPECT().Count(1).Times(1)

			stats.DefaultStats = mockStats

			job = &UploadJobT{
				upload: &UploadT{
					ID:                 uploadID,
					StartStagingFileID: 1,
					EndStagingFileID:   4,
				},
				warehouse: warehouseutils.WarehouseT{
					Type: "POSTGRES",
				},
			}
		})

		It("Success metrics", func() {
			job.generateUploadSuccessMetrics()
		})

		It("Aborted metrics", func() {
			job.generateUploadAbortedMetrics()
		})
	})

	It("Record table load", func() {
		ctrl := gomock.NewController(es)
		mockStats := mock_stats.NewMockStats(ctrl)
		mockRudderStats := mock_stats.NewMockRudderStats(ctrl)

		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(3).Return(mockRudderStats)
		mockRudderStats.EXPECT().Count(4).Times(2)
		mockRudderStats.EXPECT().SendTiming(gomock.Any()).Times(1)

		stats.DefaultStats = mockStats

		job := &UploadJobT{
			upload: &UploadT{
				ID:                 uploadID,
				StartStagingFileID: 1,
				EndStagingFileID:   4,
			},
			warehouse: warehouseutils.WarehouseT{
				Type: "POSTGRES",
			},
		}
		job.recordTableLoad("tracks", 4)
	})

	It("Record load files generation time", func() {
		ctrl := gomock.NewController(es)
		mockStats := mock_stats.NewMockStats(ctrl)
		mockRudderStats := mock_stats.NewMockRudderStats(ctrl)

		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mockRudderStats)
		mockRudderStats.EXPECT().SendTiming(gomock.Any()).Times(1)

		stats.DefaultStats = mockStats

		job := &UploadJobT{
			upload: &UploadT{
				ID:                 uploadID,
				StartStagingFileID: 1,
				EndStagingFileID:   4,
			},
			warehouse: warehouseutils.WarehouseT{
				Type: "POSTGRES",
			},
			dbHandle: pgResource.DB,
		}

		err = job.recordLoadFileGenerationTimeStat(1, 4)
		Expect(err).To(BeNil())
	})
})
