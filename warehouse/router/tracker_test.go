package router

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestRouter_Track(t *testing.T) {
	var (
		workspaceID = "test-workspaceID"
		sourceID    = "test-sourceID"
		sourceName  = "test-sourceName"
		destID      = "test-destinationID"
		destName    = "test-destinationName"
		destType    = warehouseutils.POSTGRES
	)

	testcases := []struct {
		name             string
		destID           string
		destDisabled     bool
		wantErr          error
		missing          bool
		NowSQL           string
		exclusionWindow  map[string]any
		uploadBufferTime string
	}{
		{
			name:   "unknown destination",
			destID: "unknown-destination",
		},
		{
			name:         "disabled destination",
			destID:       destID,
			destDisabled: true,
		},
		{
			name:    "successful upload exists",
			destID:  destID,
			missing: false,
		},
		{
			name:             "successful upload exists with upload buffer time",
			destID:           destID,
			missing:          false,
			uploadBufferTime: "0m",
		},
		{
			name:    "exclusion window",
			destID:  destID,
			missing: false,
			exclusionWindow: map[string]any{
				"excludeWindowStartTime": "05:09",
				"excludeWindowEndTime":   "09:07",
			},
		},
		{
			name:    "no successful upload exists",
			destID:  "test-destinationID-1",
			missing: true,
		},
		{
			name:    "throw error while fetching last upload time",
			destID:  destID,
			missing: false,
			NowSQL:  "ABC",
			wantErr: errors.New("fetching last upload time for source: test-sourceID and destination: test-destinationID: pq: column \"abc\" does not exist"),
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			t.Log("db:", pgResource.DBDsn)

			err = (&migrator.Migrator{
				Handle:          pgResource.DB,
				MigrationsTable: "wh_schema_migrations",
			}).Migrate("warehouse")
			require.NoError(t, err)

			sqlStatement, err := os.ReadFile("testdata/sql/seed_tracker_test.sql")
			require.NoError(t, err)

			_, err = pgResource.DB.Exec(string(sqlStatement))
			require.NoError(t, err)

			statsStore, err := memstats.New()
			require.NoError(t, err)

			ctx := context.Background()
			nowSQL := "'2022-12-06 15:40:00'::timestamp"

			now, err := time.Parse(misc.RFC3339Milli, "2022-12-06T06:19:00.169Z")
			require.NoError(t, err)

			conf := config.New()
			if tc.uploadBufferTime != "" {
				conf.Set("Warehouse.uploadBufferTimeInMin", tc.uploadBufferTime)
			} else {
				conf.Set("Warehouse.uploadBufferTimeInMin", 0)
			}

			warehouse := model.Warehouse{
				WorkspaceID: workspaceID,
				Source: backendconfig.SourceT{
					ID:      sourceID,
					Name:    sourceName,
					Enabled: true,
				},
				Destination: backendconfig.DestinationT{
					ID:      tc.destID,
					Name:    destName,
					Enabled: !tc.destDisabled,
					Config: map[string]any{
						"syncFrequency": "10",
						"excludeWindow": tc.exclusionWindow,
					},
				},
			}

			if tc.NowSQL != "" {
				nowSQL = tc.NowSQL
			}

			handle := Router{
				conf:     config.New(),
				destType: destType,
				now: func() time.Time {
					return now
				},
				nowSQL:       nowSQL,
				statsFactory: statsStore,
				db:           sqlquerywrapper.New(pgResource.DB),
				logger:       logger.NOP,
			}

			err = handle.Track(ctx, &warehouse, conf)
			if tc.wantErr != nil {
				require.EqualError(t, err, tc.wantErr.Error())
				return
			}
			require.NoError(t, err)

			m := statsStore.Get("warehouse_track_upload_missing", stats.Tags{
				"module":        moduleName,
				"workspaceId":   warehouse.WorkspaceID,
				"destType":      handle.destType,
				"sourceId":      warehouse.Source.ID,
				"destinationId": warehouse.Destination.ID,
				"warehouseID": misc.GetTagName(
					warehouse.Destination.ID,
					warehouse.Source.Name,
					warehouse.Destination.Name,
					misc.TailTruncateStr(warehouse.Source.ID, 6)),
			})

			if tc.missing {
				require.EqualValues(t, m.LastValue(), 1)
			} else {
				require.EqualValues(t, m.LastValue(), 0)
			}
		})
	}
}

func TestRouter_CronTracker(t *testing.T) {
	t.Run("context cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockCtrl := gomock.NewController(t)
		mockLogger := mock_logger.NewMockLogger(mockCtrl)

		statsStore, err := memstats.New()
		require.NoError(t, err)

		r := Router{
			logger:       mockLogger,
			statsFactory: statsStore,
			destType:     warehouseutils.POSTGRES,
		}

		mockLogger.EXPECT().Infon("context is cancelled, stopped running tracking").Times(1)

		executionTime := time.Now().Unix()
		err = r.CronTracker(ctx)
		require.NoError(t, err)

		m := statsStore.GetByName("warehouse_cron_tracker_timestamp_seconds")
		require.Equal(t, len(m), 1)
		require.Equal(t, m[0].Name, "warehouse_cron_tracker_timestamp_seconds")
		require.Equal(t, m[0].Tags, stats.Tags{
			"module":   moduleName,
			"destType": warehouseutils.POSTGRES,
		})
		require.GreaterOrEqual(t, m[0].Value, float64(executionTime))
	})
}
