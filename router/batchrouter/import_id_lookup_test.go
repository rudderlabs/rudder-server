package batchrouter

import (
	"context"
	stdjson "encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mockdestinationdebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/rsources"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

const importIDLookupDestType = "SNOWPIPE_STREAMING"

func TestImportIDLookup(t *testing.T) {
	ctx := context.Background()
	brt := newImportIDLookupHandle(t)

	t.Run("legacy import IDs are returned unchanged", func(t *testing.T) {
		for name, importID := range map[string]string{
			"string":       `"import-1"`,
			"object":       `{"jobId":"1","pollUrl":"https://example.com"}`,
			"array":        `[{"channelId":"c1","offset":"10","table":"T","count":1}]`,
			"uuid-looking": `"` + uuid.NewString() + `"`,
		} {
			t.Run(name, func(t *testing.T) {
				parameters := stdjson.RawMessage(`{"importId":` + importID + `,"importCount":2}`)
				resolved, err := brt.resolveImportParameters(ctx, "destination-legacy", parameters)
				require.NoError(t, err)
				require.Equal(t, string(parameters), string(resolved))
			})
		}
	})

	t.Run("a reference resolves to the original import ID byte for byte", func(t *testing.T) {
		importID := `[{"channelId": "c1", "offset":"10","table":"T","count":1}, {"channelId":"c2"}]`
		parameters := []byte(`{"importId":` + importID + `,"importCount":2,"other":{"k":"v"}}`)

		row, refParameters, err := newImportIDRef("destination-a", parameters)
		require.NoError(t, err)
		require.NotNil(t, row)
		require.JSONEq(t, `{"importIdRef":"`+row.ref+`","importCount":2,"other":{"k":"v"}}`, string(refParameters))
		storeImportID(t, brt, row)

		resolved, err := brt.resolveImportParameters(ctx, "destination-a", refParameters)
		require.NoError(t, err)
		require.Equal(t, importID, gjson.GetBytes(resolved, importIDKey).Raw)
		require.JSONEq(t, string(parameters), string(resolved))
	})

	t.Run("parameters without an import ID get no reference", func(t *testing.T) {
		parameters := []byte(`{"importCount":2}`)
		row, refParameters, err := newImportIDRef("destination-a", parameters)
		require.NoError(t, err)
		require.Nil(t, row)
		require.Equal(t, string(parameters), string(refParameters))
	})

	t.Run("a reference resolves only for its own destination", func(t *testing.T) {
		row, refParameters, err := newImportIDRef("destination-b", []byte(`{"importId":"import-b"}`))
		require.NoError(t, err)
		storeImportID(t, brt, row)

		_, err = brt.resolveImportParameters(ctx, "destination-c", refParameters)
		require.ErrorIs(t, err, errImportIDNotFound)
	})

	t.Run("storing an import keeps only the newest row of its destination", func(t *testing.T) {
		_, older := storeNewImportIDRef(t, brt, "destination-d", `"older"`)
		_, other := storeNewImportIDRef(t, brt, "destination-e", `"other"`)
		_, newer := storeNewImportIDRef(t, brt, "destination-d", `"newer"`)

		_, err := brt.resolveImportParameters(ctx, "destination-d", older)
		require.ErrorIs(t, err, errImportIDNotFound)
		requireImportID(t, brt, "destination-d", newer, `"newer"`)
		requireImportID(t, brt, "destination-e", other, `"other"`)
	})

	t.Run("storing the same row again succeeds", func(t *testing.T) {
		row, refParameters := storeNewImportIDRef(t, brt, "destination-f", `"import-f"`)
		storeImportID(t, brt, row)
		requireImportID(t, brt, "destination-f", refParameters, `"import-f"`)
	})

	t.Run("an unknown reference is not found", func(t *testing.T) {
		_, err := brt.resolveImportParameters(ctx, "destination-a", []byte(`{"importIdRef":"`+ksuid.New().String()+`"}`))
		require.ErrorIs(t, err, errImportIDNotFound)
	})
}

func TestImportIDRefs(t *testing.T) {
	ctx := context.Background()
	const destinationID = "destination-1"

	t.Run("importing statuses reference the import ID and keep per-job metadata", func(t *testing.T) {
		brt := newImportIDLookupHandle(t)
		jobs := storeImportIDLookupJobs(t, brt, destinationID, 2)

		brt.setMultipleJobStatus(setMultipleJobStatusParams{
			asyncJobMetadata: newAsyncJobMetadata(jobs),
			AsyncOutput: common.AsyncUploadOutput{
				DestinationID:          destinationID,
				ImportingJobIDs:        []int64{jobs[0].JobID, jobs[1].JobID},
				ImportingParameters:    []byte(`{"importId":[{"channelId":"c1"}],"importCount":2}`),
				JobImportingParameters: map[int64]stdjson.RawMessage{jobs[0].JobID: []byte(`{"k":"v"}`)},
			},
		})

		importing, err := brt.jobsDB.GetImporting(ctx, jobsdb.GetQueryParams{CustomValFilters: []string{importIDLookupDestType}, JobsLimit: 10}, nil)
		require.NoError(t, err)
		require.Len(t, importing.Jobs, 2)
		statusParameters := map[int64]stdjson.RawMessage{}
		for _, job := range importing.Jobs {
			statusParameters[job.JobID] = job.LastJobStatus.Parameters
		}
		ref := gjson.GetBytes(statusParameters[jobs[0].JobID], importIDRefKey).String()
		require.JSONEq(t, `{"importIdRef":"`+ref+`","importCount":2,"metadata":{"k":"v"}}`, string(statusParameters[jobs[0].JobID]))
		require.JSONEq(t, `{"importIdRef":"`+ref+`","importCount":2}`, string(statusParameters[jobs[1].JobID]))

		resolved, err := brt.resolveImportParameters(ctx, destinationID, statusParameters[jobs[0].JobID])
		require.NoError(t, err)
		require.JSONEq(t, `{"importId":[{"channelId":"c1"}],"importCount":2,"metadata":{"k":"v"}}`, string(resolved))
	})

	t.Run("a missing import ID fails the import without polling the destination", func(t *testing.T) {
		brt := newImportIDLookupHandle(t)
		missingRef := ksuid.New().String()
		storeImportingJobs(t, brt, destinationID, `{"importIdRef":"`+missingRef+`","importCount":2}`)
		// Polling would keep the jobs importing, so failed jobs show that the destination was not polled.
		startPolling(t, brt, destinationID, common.PollStatusResponse{InProgress: true})

		require.Eventually(t, func() bool {
			failed, err := brt.jobsDB.GetFailed(ctx, jobsdb.GetQueryParams{CustomValFilters: []string{importIDLookupDestType}, JobsLimit: 10})
			if err != nil || len(failed.Jobs) != 2 {
				return false
			}
			for _, job := range failed.Jobs {
				if job.LastJobStatus.ErrorCode != "500" ||
					gjson.GetBytes(job.LastJobStatus.ErrorResponse, "error").String() != "import ID not found: "+missingRef {
					return false
				}
			}
			return true
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("a lookup error keeps the import importing without polling the destination", func(t *testing.T) {
		brt := newImportIDLookupHandle(t)
		storeImportingJobs(t, brt, destinationID, `{"importIdRef":"`+ksuid.New().String()+`","importCount":2}`)
		require.NoError(t, brt.jobsDB.WithTx(ctx, func(tx *tx.Tx) error {
			_, err := tx.ExecContext(ctx, `DROP TABLE async_import_ids`)
			return err
		}))
		// Polling would complete the import, so jobs that stay importing show that the destination was not polled.
		statsStore := startPolling(t, brt, destinationID, common.PollStatusResponse{StatusCode: 200, Complete: true})

		require.Eventually(t, func() bool {
			lookupErrors := statsStore.Get("async_import_id_lookup_errors", brt.asyncDestinationTags(destinationID))
			return lookupErrors != nil && lookupErrors.LastValue() >= 3
		}, 10*time.Second, 10*time.Millisecond)
		importing, err := brt.jobsDB.GetImporting(ctx, jobsdb.GetQueryParams{CustomValFilters: []string{importIDLookupDestType}, JobsLimit: 10}, nil)
		require.NoError(t, err)
		require.Len(t, importing.Jobs, 2)
	})
}

// storeImportingJobs stores two jobs of the destination with importing statuses that carry statusParameters.
func storeImportingJobs(t *testing.T, brt *Handle, destinationID, statusParameters string) {
	t.Helper()
	var statuses []*jobsdb.JobStatusT
	for _, job := range storeImportIDLookupJobs(t, brt, destinationID, 2) {
		statuses = append(statuses, &jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      jobsdb.Importing.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(statusParameters),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
			CustomVal:     job.CustomVal,
		})
	}
	require.NoError(t, brt.jobsDB.UpdateJobStatus(context.Background(), statuses))
}

// startPolling runs the poll loop of brt until the test ends, with a destination manager whose Poll returns
// pollOutput, and returns the store of the stats it records.
func startPolling(t *testing.T, brt *Handle, destinationID string, pollOutput common.PollStatusResponse) *memstats.Store {
	t.Helper()
	statsStore := useMemStats(t)
	debugger := mockdestinationdebugger.NewMockDestinationDebugger(gomock.NewController(t))
	debugger.EXPECT().RecordEventDeliveryStatus(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	brt.debugger = debugger
	brt.pollStatusLoopSleep = config.SingleValueLoader(10 * time.Millisecond)
	brt.asyncGetImportingIterations = statsStore.NewStat("async_get_importing_iterations", stats.HistogramType)
	brt.asyncDestinationStruct[destinationID] = &common.AsyncDestinationStruct{
		Manager: &mockAsyncDestinationManager{pollOutput: pollOutput},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		brt.pollAsyncStatus(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return statsStore
}

// newImportIDLookupHandle returns a batchrouter handle with import ID references enabled, backed by a new
// postgres database with the node migrations applied.
func newImportIDLookupHandle(t *testing.T) *Handle {
	t.Helper()
	misc.Init()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	require.NoError(t, (&migrator.Migrator{Handle: pg.DB, MigrationsTable: "node_migrations"}).Migrate("node"))

	jobsDB := jobsdb.NewForReadWrite("batch_rt", jobsdb.WithDBHandle(pg.DB), jobsdb.WithConfig(config.New()))
	require.NoError(t, jobsDB.Start())
	t.Cleanup(jobsDB.TearDown)

	brt := defaultHandle(importIDLookupDestType)
	brt.jobsDB = jobsDB
	brt.rsourcesService = rsources.NewNoOpService()
	brt.writeImportIDRefs = config.SingleValueLoader(true)
	return brt
}

// storeImportIDLookupJobs stores count jobs of the destination and returns them.
func storeImportIDLookupJobs(t *testing.T, brt *Handle, destinationID string, count int) []*jobsdb.JobT {
	t.Helper()
	ctx := context.Background()
	brt.destinationsMap[destinationID] = &routerutils.DestinationWithSources{
		Destination: backendconfig.DestinationT{ID: destinationID},
		Sources:     []backendconfig.SourceT{{ID: "source-1", WorkspaceID: "workspace-1"}},
	}
	var jobs []*jobsdb.JobT
	for range count {
		jobs = append(jobs, &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       "user-1",
			WorkspaceId:  "workspace-1",
			CustomVal:    importIDLookupDestType,
			EventPayload: []byte(`{}`),
			Parameters:   []byte(`{"source_id":"source-1","destination_id":"` + destinationID + `"}`),
		})
	}
	require.NoError(t, brt.jobsDB.Store(ctx, jobs))
	stored, err := brt.jobsDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{CustomValFilters: []string{importIDLookupDestType}, JobsLimit: count})
	require.NoError(t, err)
	require.Len(t, stored.Jobs, count)
	return stored.Jobs
}

func storeImportID(t *testing.T, brt *Handle, row *asyncImportID) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, brt.jobsDB.WithTx(ctx, func(tx *tx.Tx) error {
		return row.store(ctx, tx.Tx)
	}))
}

// storeNewImportIDRef stores a new reference to importID and returns its row and reference parameters.
func storeNewImportIDRef(t *testing.T, brt *Handle, destinationID, importID string) (*asyncImportID, []byte) {
	t.Helper()
	row, refParameters, err := newImportIDRef(destinationID, []byte(`{"importId":`+importID+`}`))
	require.NoError(t, err)
	storeImportID(t, brt, row)
	return row, refParameters
}

func requireImportID(t *testing.T, brt *Handle, destinationID string, refParameters []byte, importID string) {
	t.Helper()
	resolved, err := brt.resolveImportParameters(context.Background(), destinationID, refParameters)
	require.NoError(t, err)
	require.Equal(t, importID, gjson.GetBytes(resolved, importIDKey).Raw)
}
