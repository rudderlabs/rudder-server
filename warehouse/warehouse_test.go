//go:build !warehouse_integration

package warehouse

import (
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/services/stats"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

type testingT interface {
	Setenv(key, value string)
	Cleanup(func())
	Log(...any)
}

func setupWarehouseJobs(pool *dockertest.Pool, t testingT) *destination.PostgresResource {
	pgResource, err := destination.SetupPostgres(pool, t)
	Expect(err).To(BeNil())

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
	Init2()
	Init3()
	Init4()
	Init5()
	validations.Init()
	misc.Init()
	postgres.Init()
}

func getMockStats(g GinkgoTInterface) (*mock_stats.MockStats, *mock_stats.MockMeasurement) {
	ctrl := gomock.NewController(g)
	mockStats := mock_stats.NewMockStats(ctrl)
	mockMeasurement := mock_stats.NewMockMeasurement(ctrl)
	return mockStats, mockMeasurement
}

var _ = Describe("Warehouse", func() {})

func TestUploadJob_ProcessingStats(t *testing.T) {
	testcases := []struct {
		name            string
		destType        string
		skipIdentifiers []string
		pendingJobs     int
		pickupLag       time.Duration
		wantErr         error
		Now             string
	}{
		{
			name:     "No pending jobs",
			destType: "unknown-destination",
		},
		{
			name:            "In progress namespaces",
			destType:        warehouseutils.POSTGRES,
			skipIdentifiers: []string{"test-destinationID_test-namespace"},
		},
		{
			name:        "Some pending jobs",
			destType:    warehouseutils.POSTGRES,
			pendingJobs: 3,
			pickupLag:   time.Duration(8229) * time.Second,
		},
		{
			name:     "Invalid metadata",
			destType: "test-destinationType-1",
			wantErr:  fmt.Errorf("count pending jobs: pq: invalid input syntax for type timestamp with time zone: \"\""),
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := destination.SetupPostgres(pool, t)
			require.NoError(t, err)

			err = (&migrator.Migrator{
				Handle:          pgResource.DB,
				MigrationsTable: "wh_schema_migrations",
			}).Migrate("warehouse")
			require.NoError(t, err)

			sqlStatement, err := os.ReadFile("testdata/sql/6.sql")
			require.NoError(t, err)

			_, err = pgResource.DB.Exec(string(sqlStatement))
			require.NoError(t, err)

			availableWorkers := 8
			skipIdentifierSQL := "AND ((destination_id || '_' || namespace)) != ALL($1)"
			ctx := context.Background()
			store := memstats.New()
			now := "'2022-12-06 22:00:00'"

			if len(tc.skipIdentifiers) == 0 {
				skipIdentifierSQL = ""
			}
			if len(tc.Now) != 0 {
				now = tc.Now
			}

			wh := HandleT{
				destType: tc.destType,
				Now:      now,
				stats:    store,
				dbHandle: pgResource.DB,
			}

			err = wh.processingStats(ctx, availableWorkers, tc.skipIdentifiers, skipIdentifierSQL)
			if tc.wantErr != nil {
				require.EqualError(t, err, tc.wantErr.Error())
				return
			}
			require.NoError(t, err)

			m1 := store.Get("wh_processing_pending_jobs", stats.Tags{
				"module":   moduleName,
				"destType": tc.destType,
			})
			require.EqualValues(t, m1.LastValue(), tc.pendingJobs)

			m2 := store.Get("wh_processing_available_workers", stats.Tags{
				"module":   moduleName,
				"destType": tc.destType,
			})
			require.EqualValues(t, m2.LastValue(), availableWorkers)

			m3 := store.Get("wh_processing_pickup_lag", stats.Tags{
				"module":   moduleName,
				"destType": tc.destType,
			})
			require.EqualValues(t, m3.LastDuration(), tc.pickupLag)
		})
	}
}
