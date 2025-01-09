package router

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendConfig "github.com/rudderlabs/rudder-server/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func TestRouter_CanCreateUpload(t *testing.T) {
	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
	)

	t.Run("prevScheduledTime", func(t *testing.T) {
		testCases := []struct {
			name                      string
			syncFrequency             string
			syncStartAt               string
			currTime                  time.Time
			expectedPrevScheduledTime time.Time
		}{
			{
				name:                      "should return prev scheduled time",
				syncFrequency:             "30",
				syncStartAt:               "14:00",
				currTime:                  time.Date(2020, 4, 27, 20, 23, 54, 3424534, time.UTC),
				expectedPrevScheduledTime: time.Date(2020, 4, 27, 20, 0, 0, 0, time.UTC),
			},
			{
				name:                      "should return prev scheduled time",
				syncFrequency:             "30",
				syncStartAt:               "14:00",
				currTime:                  time.Date(2020, 4, 27, 20, 30, 0, 0, time.UTC),
				expectedPrevScheduledTime: time.Date(2020, 4, 27, 20, 30, 0, 0, time.UTC),
			},
			{
				name:                      "should return prev day's last scheduled time if less than all of today's scheduled time",
				syncFrequency:             "360",
				syncStartAt:               "05:00",
				currTime:                  time.Date(2020, 4, 27, 4, 23, 54, 3424534, time.UTC),
				expectedPrevScheduledTime: time.Date(2020, 4, 26, 23, 0, 0, 0, time.UTC),
			},
			{
				name:                      "should return today's last scheduled time if current time is greater than all of today's scheduled time",
				syncFrequency:             "180",
				syncStartAt:               "22:00",
				currTime:                  time.Date(2020, 4, 27, 22, 23, 54, 3424534, time.UTC),
				expectedPrevScheduledTime: time.Date(2020, 4, 27, 22, 0, 0, 0, time.UTC),
			},
			{
				name:                      "should return appropriate scheduled time when current time is start of day",
				syncFrequency:             "180",
				syncStartAt:               "22:00",
				currTime:                  time.Date(2020, 4, 27, 0, 0, 0, 0, time.UTC),
				expectedPrevScheduledTime: time.Date(2020, 4, 26, 22, 0, 0, 0, time.UTC),
			},
			{
				name:                      "should return appropriate scheduled time when current time is start of day",
				syncFrequency:             "180",
				syncStartAt:               "00:00",
				currTime:                  time.Date(2020, 4, 27, 0, 0, 0, 0, time.UTC),
				expectedPrevScheduledTime: time.Date(2020, 4, 27, 0, 0, 0, 0, time.UTC),
			},
			{
				name:                      "should return appropriate scheduled time when current time is end of day",
				syncFrequency:             "180",
				syncStartAt:               "00:00",
				currTime:                  time.Date(2020, 4, 27, 23, 59, 59, 999999, time.UTC),
				expectedPrevScheduledTime: time.Date(2020, 4, 27, 21, 0, 0, 0, time.UTC),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				r := Router{}
				r.conf = config.New()
				r.createUploadAlways = &atomic.Bool{}
				r.scheduledTimesCache = make(map[string][]int)
				require.Equal(t, tc.expectedPrevScheduledTime, r.prevScheduledTime(tc.syncFrequency, tc.syncStartAt, tc.currTime))
			})
		}
	})

	t.Run("excludeWindowStartEndTimes", func(t *testing.T) {
		testCases := []struct {
			name          string
			excludeWindow map[string]interface{}
			expectedStart string
			expectedEnd   string
		}{
			{
				name: "nil",
			},
			{
				name: "excludeWindowStartTime and excludeWindowEndTime",
				excludeWindow: map[string]interface{}{
					"excludeWindowStartTime": "2006-01-02 15:04:05.999999 Z",
					"excludeWindowEndTime":   "2006-01-02 15:05:05.999999 Z",
				},
				expectedStart: "2006-01-02 15:04:05.999999 Z",
				expectedEnd:   "2006-01-02 15:05:05.999999 Z",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				start, end := excludeWindowStartEndTimes(tc.excludeWindow)
				require.Equal(t, tc.expectedStart, start)
				require.Equal(t, tc.expectedEnd, end)
			})
		}
	})

	t.Run("checkCurrentTimeExistsInExcludeWindow", func(t *testing.T) {
		testCases := []struct {
			currentTime   time.Time
			windowStart   string
			windowEnd     string
			expectedValue bool
		}{
			{
				currentTime:   time.Date(2009, time.November, 10, 5, 30, 0, 0, time.UTC),
				windowStart:   "05:00",
				windowEnd:     "06:00",
				expectedValue: true,
			},
			{
				currentTime:   time.Date(2009, time.November, 10, 5, 0o5, 0, 0, time.UTC),
				windowStart:   "05:00",
				windowEnd:     "06:00",
				expectedValue: true,
			},
			{
				currentTime:   time.Date(2009, time.November, 10, 5, 30, 0, 0, time.UTC),
				windowStart:   "22:00",
				windowEnd:     "06:00",
				expectedValue: true,
			},
			{
				currentTime:   time.Date(2009, time.November, 10, 23, 30, 0, 0, time.UTC),
				windowStart:   "22:00",
				windowEnd:     "06:00",
				expectedValue: true,
			},
			{
				currentTime: time.Date(2009, time.November, 10, 7, 30, 0, 0, time.UTC),
				windowStart: "05:00",
				windowEnd:   "06:00",
			},
			{
				currentTime: time.Date(2009, time.November, 10, 7, 30, 0, 0, time.UTC),
				windowStart: "22:00",
				windowEnd:   "06:00",
			},
			{
				currentTime: time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC),
				windowStart: "22:00",
				windowEnd:   "06:00",
			},
			{
				currentTime: time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC),
				windowStart: "",
				windowEnd:   "",
			},
			{
				currentTime: time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC),
				windowStart: "22:00",
				windowEnd:   "",
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("checkCurrentTimeExistsInExcludeWindow %d", i), func(t *testing.T) {
				require.Equal(t, checkCurrentTimeExistsInExcludeWindow(tc.currentTime, tc.windowStart, tc.windowEnd), tc.expectedValue)
			})
		}
	})

	t.Run("canCreateUpload", func(t *testing.T) {
		t.Run("upload triggered", func(t *testing.T) {
			w := model.Warehouse{
				Identifier: "test_identifier",
			}

			r := Router{}
			r.conf = config.New()
			r.triggerStore = &sync.Map{}
			r.triggerStore.Store(w.Identifier, struct{}{})
			r.createUploadAlways = &atomic.Bool{}
			r.scheduledTimesCache = make(map[string][]int)

			err := r.canCreateUpload(context.Background(), w)
			require.NoError(t, err)
		})

		t.Run("sync frequency ignore", func(t *testing.T) {
			t.Run("first upload", func(t *testing.T) {
				w := model.Warehouse{
					Identifier: "test_identifier_first_upload",
				}

				r := Router{}
				r.conf = config.New()
				r.config.uploadFreqInS = config.SingleValueLoader(int64(1800))
				r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
				r.triggerStore = &sync.Map{}
				r.createUploadAlways = &atomic.Bool{}
				r.scheduledTimesCache = make(map[string][]int)

				err := r.canCreateUpload(context.Background(), w)
				require.NoError(t, err)
			})

			t.Run("upload frequency exceeded", func(t *testing.T) {
				w := model.Warehouse{
					Identifier: "test_identifier_upload_frequency_exceeded",
				}

				now := timeutil.Now()

				r := Router{}
				r.conf = config.New()
				r.now = func() time.Time {
					return now
				}
				r.config.uploadFreqInS = config.SingleValueLoader(int64(1800))
				r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
				r.createJobMarkerMap = make(map[string]time.Time)
				r.triggerStore = &sync.Map{}
				r.createUploadAlways = &atomic.Bool{}
				r.scheduledTimesCache = make(map[string][]int)

				r.updateCreateJobMarker(w, now.Add(-time.Hour))

				err := r.canCreateUpload(context.Background(), w)
				require.NoError(t, err)
			})

			t.Run("upload frequency not exceeded", func(t *testing.T) {
				w := model.Warehouse{
					Identifier: "test_identifier_upload_frequency_exceeded",
				}

				now := timeutil.Now()

				r := Router{}
				r.conf = config.New()
				r.now = func() time.Time {
					return now
				}
				r.config.uploadFreqInS = config.SingleValueLoader(int64(1800))
				r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
				r.createJobMarkerMap = make(map[string]time.Time)
				r.triggerStore = &sync.Map{}
				r.createUploadAlways = &atomic.Bool{}
				r.scheduledTimesCache = make(map[string][]int)

				r.updateCreateJobMarker(w, now)

				err := r.canCreateUpload(context.Background(), w)
				require.ErrorIs(t, err, errUploadFrequencyExceeded)
			})
		})

		t.Run("check current window exists in exclude window", func(t *testing.T) {
			w := model.Warehouse{
				Identifier: "test_identifier_check_current_window",
				Destination: backendConfig.DestinationT{
					Config: map[string]interface{}{
						"excludeWindow": map[string]interface{}{
							"excludeWindowStartTime": "05:00",
							"excludeWindowEndTime":   "06:00",
						},
					},
				},
			}

			r := Router{}
			r.conf = config.New()
			r.triggerStore = &sync.Map{}
			r.createUploadAlways = &atomic.Bool{}
			r.scheduledTimesCache = make(map[string][]int)
			r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(false)
			r.now = func() time.Time {
				return time.Date(2009, time.November, 10, 5, 30, 0, 0, time.UTC)
			}

			err := r.canCreateUpload(context.Background(), w)
			require.ErrorIs(t, err, errCurrentTimeExistsInExcludeWindow)
		})

		t.Run("no sync start at and frequency not exceeded", func(t *testing.T) {
			w := model.Warehouse{
				Identifier: "test_identifier_no_sync_start_at_frequency_not_exceeded",
				Destination: backendConfig.DestinationT{
					Config: map[string]interface{}{},
				},
			}

			now := timeutil.Now()

			r := Router{}
			r.conf = config.New()
			r.now = func() time.Time {
				return now
			}
			r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(false)
			r.config.uploadFreqInS = config.SingleValueLoader(int64(1800))
			r.createJobMarkerMap = make(map[string]time.Time)
			r.triggerStore = &sync.Map{}
			r.createUploadAlways = &atomic.Bool{}
			r.scheduledTimesCache = make(map[string][]int)

			r.updateCreateJobMarker(w, now)

			err := r.canCreateUpload(context.Background(), w)
			require.ErrorIs(t, err, errUploadFrequencyExceeded)
		})

		t.Run("no sync start at and frequency exceeded", func(t *testing.T) {
			w := model.Warehouse{
				Identifier: "test_identifier_no_sync_start_at_frequency_exceeded",
				Destination: backendConfig.DestinationT{
					Config: map[string]interface{}{},
				},
			}

			now := timeutil.Now()

			r := Router{}
			r.conf = config.New()
			r.now = func() time.Time {
				return now
			}
			r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(false)
			r.config.uploadFreqInS = config.SingleValueLoader(int64(1800))
			r.triggerStore = &sync.Map{}
			r.createUploadAlways = &atomic.Bool{}
			r.scheduledTimesCache = make(map[string][]int)
			r.createJobMarkerMap = make(map[string]time.Time)

			r.updateCreateJobMarker(w, now.Add(-time.Hour))

			err := r.canCreateUpload(context.Background(), w)
			require.NoError(t, err)
		})

		t.Run("last created at", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			db := sqlmiddleware.New(pgResource.DB)

			err = (&migrator.Migrator{
				Handle:          pgResource.DB,
				MigrationsTable: "wh_schema_migrations",
			}).Migrate("warehouse")
			require.NoError(t, err)

			ctx := context.Background()

			now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
			repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
				return now
			}))
			repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
				return now
			}))

			stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
			require.NoError(t, err)

			_, err = repoUpload.CreateWithStagingFiles(ctx, model.Upload{
				SourceID:        sourceID,
				DestinationID:   destinationID,
				DestinationType: destinationType,
				Status:          model.Waiting,
			}, []*model.StagingFile{
				{
					ID:            stagingID,
					SourceID:      sourceID,
					DestinationID: destinationID,
				},
			})
			require.NoError(t, err)

			testCases := []struct {
				name    string
				now     time.Time
				wantErr error
			}{
				{
					name: "last created at is before prev schedule time",
					now:  now.Add(time.Hour),
				},
				{
					name:    "last created at is after prev schedule time",
					now:     now.Add(-time.Hour),
					wantErr: errBeforeScheduledTime,
				},
			}

			for i, tc := range testCases {
				tc := tc
				i := i

				t.Run(tc.name, func(t *testing.T) {
					w := model.Warehouse{
						Identifier: "test_identifier_last_created_at_" + strconv.Itoa(i),
						Source: backendConfig.SourceT{
							ID: sourceID,
						},
						Destination: backendConfig.DestinationT{
							ID: destinationID,
							Config: map[string]interface{}{
								"syncFrequency": "30",
								"syncStartAt":   "00:00",
							},
						},
					}

					r := Router{}
					r.conf = config.New()
					r.triggerStore = &sync.Map{}
					r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(false)
					r.createJobMarkerMap = make(map[string]time.Time)
					r.createUploadAlways = &atomic.Bool{}
					r.scheduledTimesCache = make(map[string][]int)
					r.uploadRepo = repoUpload
					r.now = func() time.Time {
						return tc.now
					}

					r.updateCreateJobMarker(w, now)

					err := r.canCreateUpload(context.Background(), w)
					if tc.wantErr != nil {
						require.ErrorIs(t, err, tc.wantErr)
						return
					}
					require.NoError(t, err)
				})
			}
		})
	})
}
