//go:build !warehouse_integration

package warehouse

import (
	"context"
	"os"

	"github.com/golang/mock/gomock"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var _ = Describe("Archiver", Ordered, func() {
	var (
		pgResource    *destination.PostgresResource
		minioResource *destination.MINIOResource
		cleanup       = &testhelper.Cleanup{}
		g             = GinkgoT()
		prefix        = "test-prefix"
	)

	BeforeAll(func() {
		pool, err := dockertest.NewPool("")
		Expect(err).To(BeNil())

		pgResource = setupWarehouseJobs(pool, GinkgoT(), cleanup)

		minioResource, err = destination.SetupMINIO(pool, cleanup)
		Expect(err).To(BeNil())

		g.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
		g.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
		g.Setenv("JOBS_BACKUP_BUCKET", minioResource.BucketName)
		g.Setenv("JOBS_BACKUP_PREFIX", prefix)
		g.Setenv("MINIO_ENDPOINT", minioResource.Endpoint)
		g.Setenv("MINIO_ACCESS_KEY_ID", minioResource.AccessKey)
		g.Setenv("MINIO_SECRET_ACCESS_KEY", minioResource.SecretKey)
		g.Setenv("MINIO_SSL", "false")
		g.Setenv("RUDDER_TMPDIR", g.TempDir())
		g.Setenv("RSERVER_WAREHOUSE_UPLOADS_ARCHIVAL_TIME_IN_DAYS", "0")

		initWarehouse()

		err = setupDB(context.TODO(), getConnectionString())
		Expect(err).To(BeNil())

		sqlStatement, err := os.ReadFile("testdata/sql/1.sql")
		Expect(err).To(BeNil())

		_, err = pgResource.DB.Exec(string(sqlStatement))
		Expect(err).To(BeNil())

		pkgLogger = logger.NOP
	})

	AfterAll(func() {
		cleanup.Run()
	})

	Describe("Archive uploads", func() {
		BeforeEach(func() {
			defaultStats := stats.Default

			DeferCleanup(func() {
				stats.Default = defaultStats
			})
		})

		It("should archive uploads", func() {
			mockStats, mockMeasurement := getMockStats(g)
			mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(4).Return(mockMeasurement)
			mockMeasurement.EXPECT().Count(1).Times(4)

			stats.Default = mockStats

			archiveUploads(dbHandle)
		})
	})
})
