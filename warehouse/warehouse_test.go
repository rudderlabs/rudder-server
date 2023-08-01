package warehouse

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"

	"github.com/rudderlabs/rudder-server/warehouse/multitenant"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/stats/mock_stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/admin"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type testingT interface { // TODO replace with testing.TB
	Setenv(key, value string)
	Cleanup(func())
	Log(...any)
	Errorf(format string, args ...interface{})
	FailNow()
}

func setupWarehouseJobsDB(pool *dockertest.Pool, t testingT) *resource.PostgresResource {
	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	t.Setenv("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
	t.Setenv("WAREHOUSE_JOBS_DB_USER", pgResource.User)
	t.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
	t.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", pgResource.Port)

	return pgResource
}

func initWarehouse() {
	config.Reset()
	admin.Init()
	logger.Reset()
	Init()
	Init4()
	validations.Init()
	misc.Init()
}

func getMockStats(g GinkgoTInterface) (*mock_stats.MockStats, *mock_stats.MockMeasurement) {
	ctrl := gomock.NewController(g)
	mockStats := mock_stats.NewMockStats(ctrl)
	mockMeasurement := mock_stats.NewMockMeasurement(ctrl)
	return mockStats, mockMeasurement
}

func TestUploadJob_ProcessingStats(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name            string
		destType        string
		skipIdentifiers []string
		pendingJobs     int
		pickupLag       time.Duration
		pickupWaitTime  time.Duration
		wantErr         error
	}{
		{
			name:     "no pending jobs",
			destType: "unknown-destination",
		},
		{
			name:            "in progress namespace",
			destType:        warehouseutils.POSTGRES,
			skipIdentifiers: []string{"test-destinationID_test-namespace"},
		},
		{
			name:           "some pending jobs",
			destType:       warehouseutils.POSTGRES,
			pendingJobs:    3,
			pickupLag:      time.Duration(3983) * time.Second,
			pickupWaitTime: time.Duration(8229) * time.Second,
		},
		{
			name:     "invalid metadata",
			destType: "test-destinationType-1",
			wantErr:  fmt.Errorf("count pending jobs: pq: invalid input syntax for type timestamp with time zone: \"\""),
		},
		{
			name:           "no next retry time",
			destType:       "test-destinationType-2",
			pendingJobs:    1,
			pickupLag:      time.Duration(0) * time.Second,
			pickupWaitTime: time.Duration(0) * time.Second,
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)

			t.Log("db:", pgResource.DBDsn)

			err = (&migrator.Migrator{
				Handle:          pgResource.DB,
				MigrationsTable: "wh_schema_migrations",
			}).Migrate("warehouse")
			require.NoError(t, err)

			sqlStatement, err := os.ReadFile("testdata/sql/processing_stats_test.sql")
			require.NoError(t, err)

			_, err = pgResource.DB.Exec(string(sqlStatement))
			require.NoError(t, err)

			availableWorkers := 8
			ctx := context.Background()
			store := memstats.New()

			wh := Router{
				destType:     tc.destType,
				stats:        store,
				dbHandle:     sqlquerywrapper.New(pgResource.DB),
				whSchemaRepo: repo.NewWHSchemas(sqlquerywrapper.New(pgResource.DB)),
			}
			tenantManager = &multitenant.Manager{}

			jobStats, err := repo.NewUploads(sqlmiddleware.New(pgResource.DB), repo.WithNow(func() time.Time {
				// nowSQL := "'2022-12-06 22:00:00'"
				return time.Date(2022, 12, 6, 22, 0, 0, 0, time.UTC)
			})).UploadJobsStats(ctx, tc.destType, repo.ProcessOptions{
				SkipIdentifiers: tc.skipIdentifiers,
				SkipWorkspaces:  nil,
			})
			if tc.wantErr != nil {
				require.EqualError(t, err, tc.wantErr.Error())
				return
			}
			require.NoError(t, err)

			wh.processingStats(availableWorkers, jobStats)

			m1 := store.Get("wh_processing_pending_jobs", stats.Tags{
				"destType": tc.destType,
			})
			require.EqualValues(t, m1.LastValue(), tc.pendingJobs)

			m2 := store.Get("wh_processing_available_workers", stats.Tags{
				"destType": tc.destType,
			})
			require.EqualValues(t, m2.LastValue(), availableWorkers)

			m3 := store.Get("wh_processing_pickup_lag", stats.Tags{
				"destType": tc.destType,
			})
			require.EqualValues(t, m3.LastDuration(), tc.pickupLag)

			m4 := store.Get("wh_processing_pickup_wait_time", stats.Tags{
				"destType": tc.destType,
			})
			require.EqualValues(t, m4.LastDuration(), tc.pickupWaitTime)
		})
	}
}
