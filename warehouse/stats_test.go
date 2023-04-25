package warehouse

import (
	"context"
	"os"

	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/testhelper"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

var _ = Describe("Stats", Ordered, func() {
	var (
		g          = GinkgoT()
		pgResource *resource.PostgresResource
		err        error
		uploadID   = int64(1)
		cleanup    = &testhelper.Cleanup{}
	)

	BeforeAll(func() {
		pool, err := dockertest.NewPool("")
		Expect(err).To(BeNil())

		pgResource = setupWarehouseJobs(pool, g)

		initWarehouse()

		err = setupDB(context.TODO(), getConnectionString())
		Expect(err).To(BeNil())

		sqlStatement, err := os.ReadFile("testdata/sql/stats_test.sql")
		Expect(err).To(BeNil())

		_, err = pgResource.DB.Exec(string(sqlStatement))
		Expect(err).To(BeNil())

		pkgLogger = logger.NOP
	})

	AfterAll(func() {
		cleanup.Run()
	})

	Describe("Generate upload success metrics", func() {
		var job *UploadJob

		BeforeEach(func() {
			mockStats, mockMeasurement := getMockStats(g)
			mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(3).Return(mockMeasurement)
			mockMeasurement.EXPECT().Count(4).Times(2)
			mockMeasurement.EXPECT().Count(1).Times(1)

			job = &UploadJob{
				upload: model.Upload{
					ID:                 uploadID,
					StagingFileStartID: 1,
					StagingFileEndID:   4,
					SourceID:           "test-sourceID",
					DestinationID:      "test-destinationID",
				},
				warehouse: model.Warehouse{
					Type: "POSTGRES",
				},
				stats:            mockStats,
				tableUploadsRepo: repo.NewTableUploads(pgResource.DB),
			}
		})

		It("Success metrics", func() {
			job.generateUploadSuccessMetrics()
		})
	})

	Describe("Generate upload aborted metrics", func() {
		var job *UploadJob

		BeforeEach(func() {
			mockStats, mockMeasurement := getMockStats(g)
			mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(2).Return(mockMeasurement)
			mockMeasurement.EXPECT().Count(4).Times(2)

			job = &UploadJob{
				upload: model.Upload{
					ID:                 uploadID,
					StagingFileStartID: 1,
					StagingFileEndID:   4,
					SourceID:           "test-sourceID",
					DestinationID:      "test-destinationID",
				},
				warehouse: model.Warehouse{
					Type: "POSTGRES",
				},
				stats:            mockStats,
				tableUploadsRepo: repo.NewTableUploads(pgResource.DB),
			}
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

		job := &UploadJob{
			upload: model.Upload{
				WorkspaceID:        "workspaceID",
				ID:                 uploadID,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
			},
			warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
			stats: mockStats,
		}
		job.recordTableLoad("tracks", 4)
	})

	It("Record load files generation time", func() {
		mockStats, mockMeasurement := getMockStats(g)
		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mockMeasurement)
		mockMeasurement.EXPECT().SendTiming(gomock.Any()).Times(1)

		job := &UploadJob{
			upload: model.Upload{
				ID:                 uploadID,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
			},
			warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
			dbHandle: pgResource.DB,
			stats:    mockStats,
		}

		err = job.recordLoadFileGenerationTimeStat(1, 4)
		Expect(err).To(BeNil())
	})
})
