package warehouse

import (
	"context"

	"github.com/ory/dockertest/v3"
	mocklogger "github.com/rudderlabs/rudder-server/mocks/utils/logger"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

func archiveSQLStatement() string {
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
				1, 1, 1, 1, 'exported_data', '{}', '{}', '{}',
				now(), now(), now(), now()
			  ),
			  (
				2, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				2, 2, 2, 2, 'exported_data',
				'{}', '{}', '{}', now(), now(), now(),
				now()
			  ),
			  (
				3, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				3, 3, 3, 3, 'exported_data', '{}', '{}', '{}',
				now(), now(), now(), now()
			  ),
			  (
				4, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				4, 4, 4, 4, 'exported_data', '{}', '{}',
				'{}', now(), now(), now(), now()
			  );
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
				4, 3, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
				'test-sourceID', 'test-destinationID',
				'POSTGRES', 'test-table', 1, NOW(),
				'{}'
			  );
			END;
	`
}

var _ = Describe("Archiver", Ordered, func() {
	var (
		pgResource    *destination.PostgresResource
		minioResource *destination.MINIOResource
		err           error
		cleanup       = &testhelper.Cleanup{}
		es            = GinkgoT()
		mockLogger    *mocklogger.MockLoggerI
		mockCtrl      *gomock.Controller
		prefix        = "test-prefix"
	)

	BeforeAll(func() {
		pool, err := dockertest.NewPool("")
		Expect(err).To(BeNil())

		pgResource = setupWarehouseJobs(pool, GinkgoT(), cleanup)

		minioResource, err = destination.SetupMINIO(pool, cleanup)
		Expect(err).To(BeNil())

		es.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
		es.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
		es.Setenv("JOBS_BACKUP_BUCKET", minioResource.BucketName)
		es.Setenv("JOBS_BACKUP_PREFIX", prefix)
		es.Setenv("MINIO_ENDPOINT", minioResource.Endpoint)
		es.Setenv("MINIO_ACCESS_KEY_ID", minioResource.AccessKey)
		es.Setenv("MINIO_SECRET_ACCESS_KEY", minioResource.SecretKey)
		es.Setenv("MINIO_SSL", "false")
		es.Setenv("RUDDER_TMPDIR", es.TempDir())
		es.Setenv("RSERVER_WAREHOUSE_UPLOADS_ARCHIVAL_TIME_IN_DAYS", "0")

		initWarehouse()

		err = setupDB(context.TODO(), getConnectionString())
		Expect(err).To(BeNil())

		mockCtrl = gomock.NewController(es)
		mockLogger = mocklogger.NewMockLoggerI(mockCtrl)
		mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).MaxTimes(0)
		mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).MinTimes(0)
		mockLogger.EXPECT().Infof(gomock.Any(), gomock.Any()).AnyTimes()
		mockLogger.EXPECT().Debugf(gomock.Any(), gomock.Any()).AnyTimes()

		pkgLogger = mockLogger
	})

	AfterAll(func() {
		cleanup.Run()
	})

	It("Archive uploads", func() {
		_, err = pgResource.DB.Exec(archiveSQLStatement())
		Expect(err).To(BeNil())

		archiveUploads(dbHandle)
	})
})
