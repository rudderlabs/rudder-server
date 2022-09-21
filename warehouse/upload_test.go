//go:build !warehouse_integration

package warehouse

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/services/stats"
	"github.com/rudderlabs/rudder-server/services/stats"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestExtractUploadErrorsByState(t *testing.T) {
	input := []struct {
		InitialErrorState []byte
		CurrentErrorState string
		CurrentError      error
		ErrorCount        int
	}{
		{
			InitialErrorState: []byte(`{}`),
			CurrentErrorState: InternalProcessingFailed,
			CurrentError:      errors.New("account locked"),
			ErrorCount:        1,
		},
		{
			InitialErrorState: []byte(`{"internal_processing_failed": {"errors": ["account locked"], "attempt": 1}}`),
			CurrentErrorState: InternalProcessingFailed,
			CurrentError:      errors.New("account locked again"),
			ErrorCount:        2,
		},
		{
			InitialErrorState: []byte(`{"internal_processing_failed": {"errors": ["account locked", "account locked again"], "attempt": 2}}`),
			CurrentErrorState: TableUploadExportingFailed,
			CurrentError:      errors.New("failed to load data because failed in earlier job"),
			ErrorCount:        1,
		},
	}

	for _, ip := range input {

		uploadErrors, err := extractAndUpdateUploadErrorsByState(ip.InitialErrorState, ip.CurrentErrorState, ip.CurrentError)
		if err != nil {
			t.Errorf("extracting upload errors by state should have passed: %v", err)
		}

		stateErrors := uploadErrors[ip.CurrentErrorState]
		// Below switch clause mirrors how we are
		// adding data in generic interface.

		var errorLength int
		switch stateErrors["errors"].(type) {
		case []string:
			errorLength = len(stateErrors["errors"].([]string))
		case []interface{}:
			errorLength = len(stateErrors["errors"].([]interface{}))
		}

		if errorLength != ip.ErrorCount {
			t.Errorf("expected error to be addded to list of state errors")
		}

		if stateErrors["attempt"].(int) != ip.ErrorCount {
			t.Errorf("expected attempts to be: %d, got: %d", ip.ErrorCount, stateErrors["attempt"].(int))
		}
	}
}

func uploadsSQLStatement() string {
	return `
			BEGIN;
			INSERT INTO wh_uploads (
			  id, source_id, namespace, destination_id,
			  destination_type, start_staging_file_id,
			  end_staging_file_id, start_load_file_id,
			  end_load_file_id, status, schema,
			  error, metadata, first_event_at,
			  last_event_at, created_at, updated_at,
			  timings
			)
			VALUES
			  (
				1, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				1, 1, 1, 1, 'waiting', '{}', '{}', '{}',
				now(), now(), now(), now(), '[{ "exporting_data": "2020-04-21 15:16:19.687716", "exported_data": "2020-04-21 15:26:34.344356"}]'
			  ),
			  (
				2, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				2, 2, 2, 2, 'exporting_data_failed',
				'{}', '{}', '{}', now(), now(), now(),
				now(), '[]'
			  ),
			  (
				3, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				3, 3, 3, 3, 'aborted', '{}', '{}', '{}',
				now(), now(), now(), now(), '[]'
			  ),
			  (
				4, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				4, 4, 4, 4, 'exported_data', '{}', '{}',
				'{}', now(), now(), now(), now(), '[]'
			  ),
			  (
				5, 'test-sourceID', 'test-namespace',
				'test-destinationID', 'POSTGRES',
				5, 5, 5, 5, 'exported_data', '{}', '{}',
				'{}', now(), now(), now(), now(), '[]'
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
			  ),
			  (
				5, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
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
			  ),
			  (
				5, 5, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
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
				1, 1, 'test-table', 'waiting', 1, '',
				NOW(), NOW()
			  ),
			  (
				2, 2, 'test-table', 'exporting_data_failed', 1, '',
				NOW(), NOW()
			  ),
			  (
				3, 3, 'test-table', 'aborted', 1, '',
				NOW(), NOW()
			  ),
			  (
				4, 4, 'test-table', 'exported_data', 1, '',
				NOW(), NOW()
			  ),
			  (
				5, 5, 'test-table', 'exported_data', 1, '',
				NOW(), NOW()
			  );
			END;
	`
}

var _ = Describe("Upload", Ordered, func() {
	var (
		sourceID        = "test-sourceID"
		destinationID   = "test-destinationID"
		destinationName = "test-destinationName"
		namespace       = "test-namespace"
		destinationType = "POSTGRES"
		es              = GinkgoT()
	)

	var (
		pgResource *destination.PostgresResource
		err        error
		cleanup    = &testhelper.Cleanup{}
		job        *UploadJobT
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

	BeforeEach(func() {
		job = &UploadJobT{
			warehouse: warehouseutils.WarehouseT{
				Type: destinationType,
				Destination: backendconfig.DestinationT{
					ID:   destinationID,
					Name: destinationName,
				},
				Source: backendconfig.SourceT{
					ID:   sourceID,
					Name: destinationName,
				},
			},
			upload: &UploadT{
				ID:                 1,
				DestinationID:      destinationID,
				SourceID:           sourceID,
				StartStagingFileID: 1,
				EndStagingFileID:   5,
				Namespace:          namespace,
			},
			stagingFileIDs: []int64{1, 2, 3, 4, 5},
			dbHandle:       pgResource.DB,
		}
	})

	It("Init warehouse", func() {
		_, err = pgResource.DB.Exec(uploadsSQLStatement())
		Expect(err).To(BeNil())
	})

	It("total rows in load files", func() {
		count := job.getTotalRowsInLoadFiles()
		Expect(count).To(BeEquivalentTo(5))
	})

	It("total rows in staging files", func() {
		count := job.getTotalRowsInStagingFiles()
		Expect(count).To(BeEquivalentTo(5))
	})

	It("Fetch pending upload status", func() {
		job.upload.ID = 5

		tus := job.fetchPendingUploadTableStatus()
		Expect(tus).NotTo(BeNil())
		Expect(tus).Should(HaveLen(2))
	})

	DescribeTable("Are all table skip errors", func(loadErrors []error, expected bool) {
		Expect(areAllTableSkipErrors(loadErrors)).To(Equal(expected))
	},
		Entry(nil, []error{}, true),
		Entry(nil, []error{&TableSkipError{}}, true),
		Entry(nil, []error{errors.New("")}, false),
	)

	DescribeTable("Get table upload status map", func(tableUploadStatuses []*TableUploadStatusT, expected map[int64]map[string]*TableUploadStatusInfoT) {
		Expect(getTableUploadStatusMap(tableUploadStatuses)).To(Equal(expected))
	},
		Entry(nil, []*TableUploadStatusT{}, map[int64]map[string]*TableUploadStatusInfoT{}),

		Entry(nil,
			[]*TableUploadStatusT{
				{
					uploadID:  1,
					tableName: "test-tableName-1",
				},
				{
					uploadID:  2,
					tableName: "test-tableName-2",
				},
			},
			map[int64]map[string]*TableUploadStatusInfoT{
				1: {
					"test-tableName-1": {},
				},
				2: {
					"test-tableName-2": {},
				},
			},
		),
	)

	It("getting tables to skip", func() {
		job.upload.ID = 5

		previousFailedMap, currentSuceededMap := job.getTablesToSkip()
		Expect(previousFailedMap).Should(HaveLen(1))
		Expect(currentSuceededMap).Should(HaveLen(0))
	})

	It("get uploads timings", func() {
		Expect(job.getUploadTimings()).To(BeEquivalentTo([]map[string]string{
			{
				"exported_data":  "2020-04-21 15:26:34.344356",
				"exporting_data": "2020-04-21 15:16:19.687716",
			},
		}))
	})

	Describe("Staging files and load files events match", func() {
		When("matched", func() {
			It("should not send stats", func() {
				ctrl := gomock.NewController(es)
				mockStats := mock_stats.NewMockStats(ctrl)
				mockRudderStats := mock_stats.NewMockRudderStats(ctrl)

				mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(0).Return(mockRudderStats)

				stats.DefaultStats = mockStats

				job.matchRowsInStagingAndLoadFiles()
			})
		})

		When("not matched", func() {
			It("should send stats", func() {
				ctrl := gomock.NewController(es)
				mockStats := mock_stats.NewMockStats(ctrl)
				mockRudderStats := mock_stats.NewMockRudderStats(ctrl)

				mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mockRudderStats)
				mockRudderStats.EXPECT().Gauge(gomock.Any()).Times(1)

				stats.DefaultStats = mockStats

				job.stagingFileIDs = []int64{1, 2}
				job.matchRowsInStagingAndLoadFiles()
			})
		})
	})
})
