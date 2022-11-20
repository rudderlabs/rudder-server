package warehouse

import (
	"context"
	"os"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ory/dockertest/v3"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ = Describe("Stats", Ordered, func() {
	var (
		sourceID        = "test-sourceID"
		destinationID   = "test-destinationID"
		destinationType = "test-desinationType"
		destinationName = "test-destinationName"
		sourceName      = "test-sourceName"
		statName        = "test-statName"
		g               = GinkgoT()
		pgResource      *destination.PostgresResource
		err             error
		uploadID        = int64(1)
		cleanup         = &testhelper.Cleanup{}
	)

	BeforeAll(func() {
		pool, err := dockertest.NewPool("")
		Expect(err).To(BeNil())

		pgResource = setupWarehouseJobs(pool, g)

		initWarehouse()

		err = setupDB(context.TODO(), getConnectionString())
		Expect(err).To(BeNil())

		sqlStatement, err := os.ReadFile("testdata/sql/2.sql")
		Expect(err).To(BeNil())

		_, err = pgResource.DB.Exec(string(sqlStatement))
		Expect(err).To(BeNil())

		pkgLogger = logger.NOP
	})

	AfterAll(func() {
		cleanup.Run()
	})

	BeforeEach(func() {
		defaultStats := stats.Default

		DeferCleanup(func() {
			stats.Default = defaultStats
		})
	})

	Describe("Jobs stats", func() {
		BeforeEach(func() {
			mockStats, mockMeasurement := getMockStats(g)
			mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mockMeasurement)
			mockMeasurement.EXPECT().Count(gomock.Any()).AnyTimes()

			stats.Default = mockStats
		})

		It("Upload status stat", func() {
			getUploadStatusStat(statName, warehouseutils.Warehouse{
				WorkspaceID: "workspaceID",
				Source:      backendconfig.SourceT{ID: sourceID, Name: sourceName},
				Destination: backendconfig.DestinationT{ID: destinationID, Name: destinationName},
				Namespace:   "",
				Type:        destinationType,
				Identifier:  "",
			})
		})

		It("Persist ssl file error stat", func() {
			persistSSLFileErrorStat("workspaceID", destinationType, destinationName, destinationID, sourceName, sourceID, "")
		})
	})

	Describe("Generate upload success/aborted metrics", func() {
		var job *UploadJobT

		BeforeEach(func() {
			mockStats, mockMeasurement := getMockStats(g)
			mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(3).Return(mockMeasurement)
			mockMeasurement.EXPECT().Count(4).Times(2)
			mockMeasurement.EXPECT().Count(1).Times(1)

			stats.Default = mockStats

			job = &UploadJobT{
				upload: &Upload{
					ID:                 uploadID,
					StartStagingFileID: 1,
					EndStagingFileID:   4,
				},
				warehouse: warehouseutils.Warehouse{
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
		mockStats, mockMeasurement := getMockStats(g)
		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(3).Return(mockMeasurement)
		mockMeasurement.EXPECT().Count(4).Times(2)
		mockMeasurement.EXPECT().Since(gomock.Any()).Times(1)

		stats.Default = mockStats

		job := &UploadJobT{
			upload: &Upload{
				WorkspaceID:        "workspaceID",
				ID:                 uploadID,
				StartStagingFileID: 1,
				EndStagingFileID:   4,
			},
			warehouse: warehouseutils.Warehouse{
				Type: "POSTGRES",
			},
		}
		job.recordTableLoad("tracks", 4)
	})

	It("Record load files generation time", func() {
		mockStats, mockMeasurement := getMockStats(g)
		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mockMeasurement)
		mockMeasurement.EXPECT().SendTiming(gomock.Any()).Times(1)

		stats.Default = mockStats

		job := &UploadJobT{
			upload: &Upload{
				ID:                 uploadID,
				StartStagingFileID: 1,
				EndStagingFileID:   4,
			},
			warehouse: warehouseutils.Warehouse{
				Type: "POSTGRES",
			},
			dbHandle: pgResource.DB,
		}

		err = job.recordLoadFileGenerationTimeStat(1, 4)
		Expect(err).To(BeNil())
	})
})
