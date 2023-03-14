package warehouse

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestHandleT_Track(t *testing.T) {
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

			pgResource, err := destination.SetupPostgres(pool, t)
			require.NoError(t, err)

			err = (&migrator.Migrator{
				Handle:          pgResource.DB,
				MigrationsTable: "wh_schema_migrations",
			}).Migrate("warehouse")
			require.NoError(t, err)

			sqlStatement, err := os.ReadFile("testdata/sql/seed_tracker_test.sql")
			require.NoError(t, err)

			_, err = pgResource.DB.Exec(string(sqlStatement))
			require.NoError(t, err)

			ctx := context.Background()
			store := memstats.New()
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

			handle := HandleT{
				destType: destType,
				Now: func() time.Time {
					return now
				},
				NowSQL:   nowSQL,
				stats:    store,
				dbHandle: pgResource.DB,
				Logger:   logger.NOP,
			}

			err = handle.Track(ctx, &warehouse, conf)
			if tc.wantErr != nil {
				require.EqualError(t, err, tc.wantErr.Error())
				return
			}
			require.NoError(t, err)

			m := store.Get("warehouse_track_upload_missing", stats.Tags{
				"module":      moduleName,
				"workspaceId": warehouse.WorkspaceID,
				"destType":    handle.destType,
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

func TestHandleT_CronTracker(t *testing.T) {
	var (
		workspaceID = "test-workspaceID"
		sourceID    = "test-sourceID"
		sourceName  = "test-sourceName"
		destID      = "test-destinationID"
		destName    = "test-destinationName"
		destType    = warehouseutils.POSTGRES
	)

	t.Run("context cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockCtrl := gomock.NewController(t)
		mockLogger := mock_logger.NewMockLogger(mockCtrl)

		wh := HandleT{
			Logger: mockLogger,
		}

		mockLogger.EXPECT().Infof("context is cancelled, stopped running tracking").Times(1)

		err := wh.CronTracker(ctx)
		require.NoError(t, err)
	})

	t.Run("track error", func(t *testing.T) {
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

		sqlStatement, err := os.ReadFile("testdata/sql/seed_tracker_test.sql")
		require.NoError(t, err)

		_, err = pgResource.DB.Exec(string(sqlStatement))
		require.NoError(t, err)

		warehouse := model.Warehouse{
			WorkspaceID: workspaceID,
			Source: backendconfig.SourceT{
				ID:      sourceID,
				Name:    sourceName,
				Enabled: true,
			},
			Destination: backendconfig.DestinationT{
				ID:      destID,
				Name:    destName,
				Enabled: true,
				Config: map[string]any{
					"syncFrequency": "10",
				},
			},
		}

		now, err := time.Parse(misc.RFC3339Milli, "2022-12-06T06:19:00.169Z")
		require.NoError(t, err)

		wh := HandleT{
			destType: destType,
			Now: func() time.Time {
				return now
			},
			NowSQL:   "ABC",
			stats:    memstats.New(),
			dbHandle: pgResource.DB,
			Logger:   logger.NOP,
		}
		wh.warehouses = append(wh.warehouses, warehouse)

		err = wh.CronTracker(context.Background())
		require.EqualError(t, err, errors.New("cron tracker failed for source: test-sourceID, destination: test-destinationID with error: fetching last upload time for source: test-sourceID and destination: test-destinationID: pq: column \"abc\" does not exist").Error())
	})
}
