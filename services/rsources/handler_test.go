package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

type postgresResource struct {
	resource    *dockertest.Resource
	db          *sql.DB
	internalDSN string
	externalDSN string
}

var defaultJobTargetKey = JobTargetKey{
	TaskRunID:     "task_run_id",
	SourceID:      "source_id",
	DestinationID: "destination_id",
}

type singleTenantFixture struct {
	resource postgresResource
	sh       JobService
}

func newSingleTenantFixture(t *testing.T) *singleTenantFixture {
	t.Helper()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	resource := newDBResource(t, pool, "", "postgres")
	config := JobServiceConfig{
		LocalHostname:       "postgres",
		MaxPoolSize:         1,
		LocalConn:           resource.externalDSN,
		Log:                 logger.NOP,
		ShouldSetupSharedDB: true,
	}
	return &singleTenantFixture{
		resource: resource,
		sh:       createService(t, config),
	}
}

type multiTenantFixture struct {
	pool               *dockertest.Pool
	network            *docker.Network
	pgA, pgB, pgC      postgresResource
	configA, configB   JobServiceConfig
	serviceA, serviceB JobService
}

func newMultiTenantFixture(t *testing.T) *multiTenantFixture {
	t.Helper()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	f := &multiTenantFixture{pool: pool}
	f.network = newDockerNetwork(t, pool)
	postgres1Hostname := randomString() + "-1"
	postgres2Hostname := randomString() + "-2"
	postgres3Hostname := randomString() + "-3"
	f.pgA = newDBResource(t, pool, f.network.ID, postgres1Hostname, "wal_level=logical")
	f.pgB = newDBResource(t, pool, f.network.ID, postgres2Hostname, "wal_level=logical")
	f.pgC = newDBResource(t, pool, f.network.ID, postgres3Hostname)

	f.configA = JobServiceConfig{
		LocalHostname:          postgres1Hostname,
		MaxPoolSize:            1,
		LocalConn:              f.pgA.externalDSN,
		SharedConn:             f.pgC.externalDSN,
		SubscriptionTargetConn: f.pgA.internalDSN,
		Log:                    logger.NOP,
		ShouldSetupSharedDB:    true,
	}

	f.configB = JobServiceConfig{
		LocalHostname:          postgres2Hostname,
		MaxPoolSize:            1,
		LocalConn:              f.pgB.externalDSN,
		SharedConn:             f.pgC.externalDSN,
		SubscriptionTargetConn: f.pgB.internalDSN,
		Log:                    logger.NOP,
		ShouldSetupSharedDB:    true,
	}

	// Start 2 JobServices
	// 1. js1 with local=pgA, remote=pgC
	// 2. js2 with local=pgB, remote=pgC
	f.serviceA = createService(t, f.configA)
	f.serviceB = createService(t, f.configB)
	return f
}

type monitoringFixture struct {
	pgA, pgB postgresResource
	serviceA JobService
}

func newMonitoringFixture(t *testing.T) *monitoringFixture {
	t.Helper()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	network := newDockerNetwork(t, pool)

	postgres1Hostname := randomString() + "-1"
	postgres2Hostname := randomString() + "-2"
	f := &monitoringFixture{
		pgA: newDBResource(t, pool, network.ID, postgres1Hostname, "wal_level=logical"),
		pgB: newDBResource(t, pool, network.ID, postgres2Hostname),
	}

	configA := JobServiceConfig{
		LocalHostname:          postgres1Hostname,
		MaxPoolSize:            1,
		LocalConn:              f.pgA.externalDSN,
		SharedConn:             f.pgB.externalDSN,
		SubscriptionTargetConn: f.pgA.internalDSN,
		Log:                    logger.NOP,
		ShouldSetupSharedDB:    true,
	}
	f.serviceA = createService(t, configA)
	return f
}

// startMonitor runs Monitor in the background and returns a stop function that
// waits for it to have returned before resetting the config. Monitor re-reads
// the global config on every tick, so resetting it while the goroutine is still
// alive is a data race.
func (f *monitoringFixture) startMonitor(t *testing.T, lagGauge, replicationSlotGauge Gauger) func() {
	config.Set("Rsources.stats.monitoringInterval", 10*time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.serviceA.Monitor(ctx, lagGauge, replicationSlotGauge)
	}()
	return func() {
		cancel()
		<-done
		config.Reset()
	}
}

func TestUsingSourcesHandler(t *testing.T) {
	var noPaging PagingInfo
	t.Run("single-tenant setup with a single local datasource", func(t *testing.T) {
		stats := Stats{
			In:     10,
			Out:    4,
			Failed: 6,
		}

		t.Run("should be able to add and get failed records", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			// The captured error messages round-trip through the error_response column
			// verbatim: a structured message stays a string, a message at the cap the
			// delegate enforces is not truncated any further by the column, and sql
			// metacharacters land as data because they travel as bind parameters.
			// A record the delegate captured nothing for reads back as an empty message.
			records := []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`), Code: 422, Error: `{"message":"invalid email"}`},
				{Record: []byte(`{"record-2": "id-2"}`), Code: 410, Error: `'; DROP TABLE rsources_failed_keys_v2_records; -- \`},
				{Record: []byte(`{"record-3": "id-3"}`), Code: 500, Error: strings.Repeat("x", defaultMaxErrorLength)},
				{Record: []byte(`{"record-4": "id-4"}`), Code: 500},
			}
			jobRunId := newJobRunId()
			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, records)
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}

			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err, "it should be able to get failed records")
			expcetedRecords := JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{{
					ID: "task_run_id",
					Sources: []SourceFailedRecords[FailedRecord]{{
						ID: "source_id",
						Destinations: []DestinationFailedRecords[FailedRecord]{{
							ID:      "destination_id",
							Records: records,
						}},
					}},
				}},
			}
			require.Equal(t, expcetedRecords, failedRecords, "it should be able to get failed records")
		})

		t.Run("should read records written before the error_response column as having no message", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			// During a rolling deploy the other pods still run the previous binary,
			// whose insert does not mention the column at all. Those rows have to come
			// back as "no message captured" rather than failing the read.
			jobRunId := newJobRunId()
			_, err := f.resource.db.Exec(
				`INSERT INTO rsources_failed_keys_v2 (id, job_run_id, task_run_id, source_id, destination_id)
				 VALUES ($1, $2, $3, $4, $5)`,
				"legacy-id", jobRunId, defaultJobTargetKey.TaskRunID,
				defaultJobTargetKey.SourceID, defaultJobTargetKey.DestinationID)
			require.NoError(t, err)
			_, err = f.resource.db.Exec(
				`INSERT INTO rsources_failed_keys_v2_records (id, record_id, code) VALUES ($1, $2, $3)`,
				"legacy-id", `"rec-old"`, 422)
			require.NoError(t, err)

			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, JobFilter{}, noPaging)
			require.NoError(t, err)
			require.Equal(t, []FailedRecord{
				{Record: []byte(`"rec-old"`), Code: 422, Error: ""},
			}, failedRecords.Tasks[0].Sources[0].Destinations[0].Records)
		})

		t.Run("should be able to add and get failed records using pagination", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			jobRunId := newJobRunId()
			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`"id-1"`)},
				{Record: []byte(`"id-2"`)},
				{Record: []byte(`"id-3"`)},
				{Record: []byte(`"id-4"`)},
			})
			jobFilters := JobFilter{
				SourceID:  []string{defaultJobTargetKey.SourceID},
				TaskRunID: []string{defaultJobTargetKey.TaskRunID},
			}
			paging := PagingInfo{
				Size: 2,
			}
			for i := range 2 {
				failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, paging)
				require.NoError(t, err, "it should be able to get failed records")
				require.NotNil(t, failedRecords.Paging, "paging should not be nil")
				paging = *failedRecords.Paging
				expectedRecords := JobFailedRecordsV2{
					ID: jobRunId,
					Tasks: []TaskFailedRecords[FailedRecord]{{
						ID: "task_run_id",
						Sources: []SourceFailedRecords[FailedRecord]{{
							ID: defaultJobTargetKey.SourceID,
							Destinations: []DestinationFailedRecords[FailedRecord]{{
								ID: defaultJobTargetKey.DestinationID,
								Records: []FailedRecord{
									{Record: fmt.Appendf(nil, `"id-%d"`, (2*i)+1)},
									{Record: fmt.Appendf(nil, `"id-%d"`, (2*i)+2)},
								},
							}},
						}},
					}},
					Paging: failedRecords.Paging,
				}
				require.Equal(t, expectedRecords, failedRecords, "it should be able to get failed records")

			}
			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, paging)
			require.NoError(t, err, "it should be able to get failed records")
			require.Empty(t, failedRecords.Tasks, "last page should be empty")
			require.Nil(t, failedRecords.Paging, "last page should have no paging")
		})

		t.Run("should be able to add and get failed records in multiple batches", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			handler := f.sh.(*sourcesHandler)
			previous := handler.config.FailedRecordsInsertBatchSize
			handler.config.FailedRecordsInsertBatchSize = config.GetReloadableIntVar(3, 1, "Rsources.failedRecordsInsertBatchSize")
			defer func() { handler.config.FailedRecordsInsertBatchSize = previous }()

			jobRunId := newJobRunId()
			records := make([]FailedRecord, 7)
			expectedRecords := make([]FailedRecord, 7)
			for i := range records {
				record := fmt.Appendf(nil, `{"record-%d": "id-%d"}`, i+1, i+1)
				// Each record carries its own message, so a batch boundary that lost or
				// shifted one would be visible rather than silently uniform.
				message := fmt.Sprintf("message %d", i+1)
				records[i] = FailedRecord{Record: record, Code: i + 1, Error: message}
				expectedRecords[i] = FailedRecord{Record: record, Code: i + 1, Error: message}
			}
			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, records)

			jobFilters := JobFilter{
				SourceID:  []string{defaultJobTargetKey.SourceID},
				TaskRunID: []string{defaultJobTargetKey.TaskRunID},
			}
			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err, "it should be able to get failed records")
			require.Equal(t, JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{{
					ID: defaultJobTargetKey.TaskRunID,
					Sources: []SourceFailedRecords[FailedRecord]{{
						ID: defaultJobTargetKey.SourceID,
						Destinations: []DestinationFailedRecords[FailedRecord]{{
							ID:      defaultJobTargetKey.DestinationID,
							Records: expectedRecords,
						}},
					}},
				}},
			}, failedRecords)
		})

		t.Run("should dedup records with same record ids within a single batch", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			// The duplicates carry different messages, so the dedup's tie-break is
			// observable: the first occurrence is the one that is stored.
			jobRunId := newJobRunId()
			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`), Code: 1, Error: "first wins"},
				{Record: []byte(`{"record-1": "id-1"}`), Code: 1, Error: "second is dropped"},
				{Record: []byte(`{"record-1": "id-1"}`), Code: 1, Error: "third is dropped"},
				{Record: []byte(`{"record-2": "id-2"}`), Code: 2, Error: "other message"},
				{Record: []byte(`{"record-2": "id-2"}`), Code: 2, Error: "other, dropped"},
			})
			jobFilters := JobFilter{
				SourceID:  []string{defaultJobTargetKey.SourceID},
				TaskRunID: []string{defaultJobTargetKey.TaskRunID},
			}
			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err, "it should not fail with cardinality violation when duplicates exist in the same batch")
			require.Equal(t, JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{{
					ID: defaultJobTargetKey.TaskRunID,
					Sources: []SourceFailedRecords[FailedRecord]{{
						ID: defaultJobTargetKey.SourceID,
						Destinations: []DestinationFailedRecords[FailedRecord]{{
							ID: defaultJobTargetKey.DestinationID,
							Records: []FailedRecord{
								{Record: []byte(`{"record-1": "id-1"}`), Code: 1, Error: "first wins"},
								{Record: []byte(`{"record-2": "id-2"}`), Code: 2, Error: "other message"},
							},
						}},
					}},
				}},
			}, failedRecords, "it should return only the unique records once, keeping the first message")
		})

		t.Run("should dedup records with same record ids spanning multiple batches", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			handler := f.sh.(*sourcesHandler)
			previous := handler.config.FailedRecordsInsertBatchSize
			handler.config.FailedRecordsInsertBatchSize = config.GetReloadableIntVar(3, 1, "Rsources.failedRecordsInsertBatchSize")
			defer func() { handler.config.FailedRecordsInsertBatchSize = previous }()

			jobRunId := newJobRunId()
			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`), Code: 1},
				{Record: []byte(`{"record-1": "id-1"}`), Code: 1},
				{Record: []byte(`{"record-2": "id-2"}`), Code: 2},
				{Record: []byte(`{"record-1": "id-1"}`), Code: 1},
				{Record: []byte(`{"record-2": "id-2"}`), Code: 2},
				{Record: []byte(`{"record-3": "id-3"}`), Code: 3},
			})
			jobFilters := JobFilter{
				SourceID:  []string{defaultJobTargetKey.SourceID},
				TaskRunID: []string{defaultJobTargetKey.TaskRunID},
			}
			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err, "it should not fail with cardinality violation when duplicates exist across batches")
			require.Equal(t, JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{{
					ID: defaultJobTargetKey.TaskRunID,
					Sources: []SourceFailedRecords[FailedRecord]{{
						ID: defaultJobTargetKey.SourceID,
						Destinations: []DestinationFailedRecords[FailedRecord]{{
							ID: defaultJobTargetKey.DestinationID,
							Records: []FailedRecord{
								{Record: []byte(`{"record-1": "id-1"}`), Code: 1},
								{Record: []byte(`{"record-2": "id-2"}`), Code: 2},
								{Record: []byte(`{"record-3": "id-3"}`), Code: 3},
							},
						}},
					}},
				}},
			}, failedRecords, "it should return only the unique records once")
		})

		t.Run("shouldn't be able to get failed records when failed records collection is disabled", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			handler := f.sh.(*sourcesHandler)
			previous := handler.config.SkipFailedRecordsCollection
			handler.config.SkipFailedRecordsCollection = true
			defer func() { handler.config.SkipFailedRecordsCollection = previous }()

			jobRunId := newJobRunId()
			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})

			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, JobFilter{}, noPaging)
			require.Error(t, err, "it shouldn't be able to get failed records")
			require.Equal(t, JobFailedRecordsV2{ID: jobRunId}, failedRecords, "it should return an empty failed records")
			require.Equal(t, ErrOperationNotSupported, err, "it should return an ErrOperationNotSupported error")
		})

		t.Run("should be able to get the status", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			jobRunId := newJobRunId()
			increment(t, f.resource.db, jobRunId, defaultJobTargetKey, stats, f.sh)
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := f.sh.GetStatus(t.Context(), jobRunId, jobFilters)
			require.NoError(t, err)
			expected := JobStatus{
				ID: jobRunId,
				TasksStatus: []TaskStatus{
					{
						ID: "task_run_id",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id",
								Stats:     Stats{},
								Completed: true,
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Completed: true,
										Stats:     stats,
									},
								},
							},
						},
					},
				},
			}
			require.Equal(t, expected, status)
		})

		t.Run("should be able to delete failed keys", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			jobRunId := newJobRunId()
			increment(t, f.resource.db, jobRunId, defaultJobTargetKey, stats, f.sh)
			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			err := f.sh.DeleteFailedRecords(t.Context(), jobRunId, JobFilter{})
			require.NoError(t, err, "it should be able to delete failed keys for the jobrunid")
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := f.sh.GetStatus(t.Context(), jobRunId, jobFilters)
			require.NoError(t, err)
			require.Equal(t, JobStatus{
				ID: jobRunId,
				TasksStatus: []TaskStatus{
					{
						ID: "task_run_id",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id",
								Completed: true,
								Stats:     Stats{In: 0, Out: 0, Failed: 0},
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Completed: true,
										Stats:     Stats{In: 10, Out: 4, Failed: 6},
									},
								},
							},
						},
					},
				},
			}, status)
			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err)
			require.Equal(t, JobFailedRecordsV2{ID: jobRunId}, failedRecords)
		})

		t.Run("should be able to delete stats only", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			otherJobTargetKey := defaultJobTargetKey
			otherJobTargetKey.SourceID = "other_source_id"
			jobRunId := newJobRunId()
			increment(t, f.resource.db, jobRunId, defaultJobTargetKey, stats, f.sh)
			increment(t, f.resource.db, jobRunId, otherJobTargetKey, stats, f.sh)

			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			addFailedRecords(t, f.resource.db, jobRunId, otherJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			err := f.sh.DeleteJobStatus(t.Context(), jobRunId, JobFilter{SourceID: []string{"other_source_id"}})
			require.NoError(t, err, "it should be able to delete stats for the jobrunid")

			jobFilters := JobFilter{
				SourceID:  []string{"other_source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := f.sh.GetStatus(t.Context(), jobRunId, jobFilters)
			require.Error(t, err)
			require.Equal(t, JobStatus{}, status)
			require.ErrorIs(t, err, ErrStatusNotFound, "it should return a StatusNotFoundError")
			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err)
			require.Equal(t,
				JobFailedRecordsV2{
					ID: jobRunId,
					Tasks: []TaskFailedRecords[FailedRecord]{
						{
							ID: "task_run_id",
							Sources: []SourceFailedRecords[FailedRecord]{
								{
									ID: "other_source_id",
									Destinations: []DestinationFailedRecords[FailedRecord]{
										{
											ID: "destination_id",
											Records: []FailedRecord{
												{Record: []byte(`{"record-1": "id-1"}`)},
												{Record: []byte(`{"record-2": "id-2"}`)},
											},
										},
									},
								},
							},
						},
					},
				},
				failedRecords,
			)

			jobFilters.SourceID = []string{defaultJobTargetKey.SourceID}
			_, err = f.sh.GetStatus(t.Context(), jobRunId, jobFilters)
			require.NoError(t, err)
			failedRecords, err = f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err)
			require.NotEqual(t, JobFailedRecordsV2{ID: jobRunId}, failedRecords)
		})

		t.Run("should be able to delete stats and failed keys partially", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			otherJobTargetKey := defaultJobTargetKey
			otherJobTargetKey.SourceID = "other_source_id"
			jobRunId := newJobRunId()
			increment(t, f.resource.db, jobRunId, defaultJobTargetKey, stats, f.sh)
			increment(t, f.resource.db, jobRunId, otherJobTargetKey, stats, f.sh)

			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			addFailedRecords(t, f.resource.db, jobRunId, otherJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			err := f.sh.DeleteJobStatus(t.Context(), jobRunId, JobFilter{SourceID: []string{"other_source_id"}})
			require.NoError(t, err, "it should be able to delete stats for the jobrunid")

			err = f.sh.DeleteFailedRecords(t.Context(), jobRunId, JobFilter{SourceID: []string{"other_source_id"}})
			require.NoError(t, err, "it should be able to delete failed keys for the jobrunid")

			jobFilters := JobFilter{
				SourceID:  []string{"other_source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := f.sh.GetStatus(t.Context(), jobRunId, jobFilters)
			require.Error(t, err)
			require.Equal(t, JobStatus{}, status)
			require.ErrorIs(t, err, ErrStatusNotFound, "it should return a StatusNotFoundError")
			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err)
			require.Equal(t, JobFailedRecordsV2{ID: jobRunId}, failedRecords)

			jobFilters.SourceID = []string{defaultJobTargetKey.SourceID}
			_, err = f.sh.GetStatus(t.Context(), jobRunId, jobFilters)
			require.NoError(t, err)
			failedRecords, err = f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err)
			require.NotEqual(t, JobFailedRecordsV2{ID: jobRunId}, failedRecords)
		})

		t.Run("shouldn't be able to delete stats for an incomplete source", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			jobRunId := newJobRunId()
			stats := Stats{
				In:     10,
				Out:    4,
				Failed: 5,
			}
			increment(t, f.resource.db, jobRunId, defaultJobTargetKey, stats, f.sh)

			err := f.sh.DeleteJobStatus(t.Context(), jobRunId, JobFilter{SourceID: []string{defaultJobTargetKey.SourceID}})
			require.Equal(t, ErrSourceNotCompleted, err, "it shouldn't be able to delete stats for an incomplete source")
		})

		t.Run("shouldn't be able to delete stats for an invalid job", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			jobRunId := newJobRunId()
			stats := Stats{
				In:     10,
				Out:    4,
				Failed: 5,
			}
			increment(t, f.resource.db, jobRunId, defaultJobTargetKey, stats, f.sh)

			err := f.sh.DeleteJobStatus(t.Context(), "invalidJobRunId", JobFilter{SourceID: []string{defaultJobTargetKey.SourceID}})
			require.Equal(t, ErrStatusNotFound, err, "it shouldn't be able to delete stats for an invalid jobrunid")
		})

		t.Run("should be able to get failed records by filtering", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			jobRunId := newJobRunId()

			addFailedRecords(t, f.resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			addFailedRecords(t, f.resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, f.sh, []FailedRecord{
				{Record: []byte(`{"record-11": "id-112"}`)},
				{Record: []byte(`{"record-22": "id-222"}`)},
			})
			addFailedRecords(t, f.resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, f.sh, []FailedRecord{
				{Record: []byte(`{"record-12": "id-12"}`)},
				{Record: []byte(`{"record-21": "id-21"}`)},
			})
			addFailedRecords(t, f.resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, f.sh, []FailedRecord{
				{Record: []byte(`{"record-11": "id-11"}`)},
				{Record: []byte(`{"record-32": "id-32"}`)},
			})
			addFailedRecords(t, f.resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id3",
				DestinationID: "destination_id",
			}, f.sh, []FailedRecord{
				{Record: []byte(`{"record-13": "id-13"}`)},
				{Record: []byte(`{"record-23": "id-23"}`)},
			})
			expected := JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{
					{
						ID: "task_run_id1",
						Sources: []SourceFailedRecords[FailedRecord]{
							{
								ID: "source_id1",
								Destinations: []DestinationFailedRecords[FailedRecord]{{
									ID: "destination_id",
									Records: []FailedRecord{
										{Record: []byte(`{"record-1": "id-1"}`)},
										{Record: []byte(`{"record-11": "id-112"}`)},
										{Record: []byte(`{"record-2": "id-2"}`)},
										{Record: []byte(`{"record-22": "id-222"}`)},
									},
								}},
							},
							{
								ID: "source_id2",
								Destinations: []DestinationFailedRecords[FailedRecord]{{
									ID: "destination_id",
									Records: []FailedRecord{
										{Record: []byte(`{"record-12": "id-12"}`)},
										{Record: []byte(`{"record-21": "id-21"}`)},
									},
								}},
							},
						},
					},
					{
						ID: "task_run_id2",
						Sources: []SourceFailedRecords[FailedRecord]{{
							ID: "source_id2",
							Destinations: []DestinationFailedRecords[FailedRecord]{{
								ID: "destination_id",
								Records: []FailedRecord{
									{Record: []byte(`{"record-11": "id-11"}`)},
									{Record: []byte(`{"record-32": "id-32"}`)},
								},
							}},
						}},
					},
				},
			}
			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, JobFilter{
				SourceID:  []string{"source_id1", "source_id2"},
				TaskRunID: []string{"task_run_id1", "task_run_id2"},
			}, noPaging)
			require.NoError(t, err)
			require.Equal(t, expected, failedRecords, "it should return the failed records for the jobrunid based on the filtering")
		})

		t.Run("should be able to get the status with filtering", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			ctx := t.Context()
			jobRunId := newJobRunId()
			increment(t, f.resource.db, jobRunId, defaultJobTargetKey, stats, f.sh)
			var g errgroup.Group
			g.Go(func() error {
				return incrementStats(ctx, f.resource.db, jobRunId, JobTargetKey{
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id1",
					DestinationID: "destination_id",
				}, Stats{
					In:     10,
					Out:    0,
					Failed: 0,
				}, f.sh,
				)
			})
			g.Go(func() error {
				return incrementStats(ctx, f.resource.db, jobRunId, JobTargetKey{
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id1",
					DestinationID: "destination_id",
				}, Stats{
					In:     0,
					Out:    4,
					Failed: 6,
				}, f.sh,
				)
			})
			g.Go(func() error {
				return incrementStats(ctx, f.resource.db, jobRunId, JobTargetKey{
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id2",
					DestinationID: "destination_id",
				}, Stats{
					In:     10,
					Out:    2,
					Failed: 6,
				}, f.sh,
				)
			})
			g.Go(func() error {
				return incrementStats(ctx, f.resource.db, jobRunId, JobTargetKey{
					TaskRunID:     "task_run_id2",
					SourceID:      "source_id2",
					DestinationID: "destination_id",
				}, Stats{
					In:     10,
					Out:    2,
					Failed: 6,
				}, f.sh,
				)
			})
			g.Go(func() error {
				return incrementStats(ctx, f.resource.db, jobRunId, JobTargetKey{
					TaskRunID:     "task_run_id2",
					SourceID:      "source_id3",
					DestinationID: "destination_id",
				}, Stats{
					In:     10,
					Out:    2,
					Failed: 6,
				}, f.sh,
				)
			})
			require.NoError(t, g.Wait())

			res, err := f.sh.GetStatus(ctx, jobRunId, JobFilter{
				SourceID:  []string{"source_id1", "source_id2"},
				TaskRunID: []string{"task_run_id1", "task_run_id2"},
			})
			require.NoError(t, err)
			sort.Slice(res.TasksStatus, func(i, j int) bool {
				return res.TasksStatus[i].ID < res.TasksStatus[j].ID
			})
			for _, ts := range res.TasksStatus {
				sort.Slice(ts.SourcesStatus, func(i, j int) bool {
					return ts.SourcesStatus[i].ID < ts.SourcesStatus[j].ID
				})
			}

			expected := JobStatus{
				ID: jobRunId,
				TasksStatus: []TaskStatus{
					{
						ID: "task_run_id1",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id1",
								Completed: true,
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Completed: true,
										Stats: Stats{
											In:     10,
											Out:    4,
											Failed: 6,
										},
									},
								},
							},
							{
								ID:        "source_id2",
								Completed: false,
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Completed: false,
										Stats: Stats{
											In:     10,
											Out:    2,
											Failed: 6,
										},
									},
								},
							},
						},
					},
					{
						ID: "task_run_id2",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id2",
								Completed: false,
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Completed: false,
										Stats: Stats{
											In:     10,
											Out:    2,
											Failed: 6,
										},
									},
								},
							},
						},
					},
				},
			}
			require.Equal(t, expected, res)
		})

		t.Run("should be able to execute the cleanup loop", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			jobRunId := newJobRunId()
			increment(t, f.resource.db, jobRunId, defaultJobTargetKey, stats, f.sh)
			addFailedRecords(t, f.resource.db, jobRunId, defaultJobTargetKey, f.sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			ts := time.Now().Add(-time.Duration(defaultRetentionPeriodInHours+1) * time.Hour)
			stmt, err := f.resource.db.Prepare(`update "rsources_stats" set ts = $1`)
			require.NoError(t, err)
			_, err = stmt.Exec(ts)
			require.NoError(t, err)
			defer func() { _ = stmt.Close() }()
			stmt2, err := f.resource.db.Prepare(`update "rsources_failed_keys_v2_records" set ts = $1`)
			require.NoError(t, err)
			_, err = stmt2.Exec(ts)
			defer func() { _ = stmt2.Close() }()
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
			cleanupLoopDone := make(chan struct{})
			go func() {
				defer close(cleanupLoopDone)
				_ = f.sh.CleanupLoop(ctx)
			}()
			defer func() {
				cancel()
				<-cleanupLoopDone
			}()

			for {
				select {
				case <-ctx.Done():
					t.Fatal("it should cleanup all tables")
					return
				case <-time.After(1 * time.Second):
					sqlStatement := `select count(*) from "rsources_stats" where job_run_id = $1`
					var statCount int
					err = f.resource.db.QueryRow(sqlStatement, jobRunId).Scan(&statCount)
					require.NoError(t, err)

					sqlStatement = `select count(*) from "rsources_failed_keys_v2" k join "rsources_failed_keys_v2_records" v on v.id = k.id where k.job_run_id = $1`
					var failedRecordCount int
					err = f.resource.db.QueryRow(sqlStatement, jobRunId).Scan(&failedRecordCount)
					require.NoError(t, err)

					if statCount == 0 && failedRecordCount == 0 {
						return
					}
				}
			}
		})

		t.Run("should be able to add failed concurrently", func(t *testing.T) {
			f := newSingleTenantFixture(t)
			jobRunId := newJobRunId()
			tx0, err := f.resource.db.Begin()
			require.NoError(t, err, "it should be able to begin the transaction")
			// Create a record in the rsources_failed_keys_v2 table without committing the transaction
			_, err = tx0.Exec("INSERT INTO rsources_failed_keys_v2 (id, job_run_id, task_run_id, source_id, destination_id) VALUES ($1, $2, $3, $4, $5)",
				ksuid.New().String(), jobRunId, defaultJobTargetKey.TaskRunID, defaultJobTargetKey.SourceID, defaultJobTargetKey.DestinationID)
			require.NoError(t, err, "it should be able to insert a record in the rsources_failed_keys_v2 table")
			// Start 2 goroutines both trying to add failed records for the same job target key
			ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
			defer cancel()
			g, ctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				tx1, err := f.resource.db.BeginTx(ctx, nil)
				if err != nil {
					return fmt.Errorf("beginning tx1: %w", err)
				}
				if err := f.sh.AddFailedRecords(ctx, tx1, jobRunId, defaultJobTargetKey, []FailedRecord{
					{Record: []byte(`"record-1"`)},
					{Record: []byte(`"record-2"`)},
				}); err != nil {
					return fmt.Errorf("adding failed records during tx1: %w", err)
				}
				if err := tx1.Commit(); err != nil {
					return fmt.Errorf("committing tx1: %w", err)
				}
				return nil
			})
			g.Go(func() error {
				tx2, err := f.resource.db.Begin()
				if err != nil {
					return fmt.Errorf("beginning tx2: %w", err)
				}
				if err := f.sh.AddFailedRecords(ctx, tx2, jobRunId, defaultJobTargetKey, []FailedRecord{
					{Record: []byte(`"record-2"`)},
					{Record: []byte(`"record-3"`)},
				}); err != nil {
					return fmt.Errorf("adding failed records during tx2: %w", err)
				}
				if err := tx2.Commit(); err != nil {
					return fmt.Errorf("committing tx2: %w", err)
				}
				return nil
			})

			// wait for the goroutines to block on the query
			time.Sleep(1 * time.Second)

			// rollback tx0 so that the goroutines can continue
			require.NoError(t, tx0.Rollback(), "it should be able to rollback tx0")
			require.NoError(t, g.Wait(), "it should be able to add failed records concurrently")

			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}

			failedRecords, err := f.sh.GetFailedRecords(t.Context(), jobRunId, jobFilters, noPaging)
			require.NoError(t, err, "it should be able to get failed records")
			expcetedRecords := JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{{
					ID: "task_run_id",
					Sources: []SourceFailedRecords[FailedRecord]{{
						ID: "source_id",
						Destinations: []DestinationFailedRecords[FailedRecord]{{
							ID: "destination_id",
							Records: []FailedRecord{
								{Record: []byte(`"record-1"`)},
								{Record: []byte(`"record-2"`)},
								{Record: []byte(`"record-3"`)},
							},
						}},
					}},
				}},
			}
			require.Equal(t, expcetedRecords, failedRecords, "it should be able to get failed records")
		})
	})

	t.Run("multitenant setup with local & shared datasources", func(t *testing.T) {
		t.Run("should be able to query both services for the same jobRunId and receive same stats", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			// Status from both services should be same
			jobRunId := newJobRunId()
			statsA := Stats{
				In:     5,
				Out:    4,
				Failed: 0,
			}
			increment(t, f.pgA.db, jobRunId, defaultJobTargetKey, statsA, f.serviceA)

			statsB := Stats{
				In:     3,
				Out:    2,
				Failed: 1,
			}
			increment(t, f.pgB.db, jobRunId, defaultJobTargetKey, statsB, f.serviceB)
			require.Eventually(t, func() bool {
				totalStatsA, err := f.serviceA.GetStatus(t.Context(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				if err != nil {
					return false
				}
				totalStatsB, err := f.serviceB.GetStatus(t.Context(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				if err != nil {
					return false
				}
				expected := JobStatus{
					ID: jobRunId,
					TasksStatus: []TaskStatus{
						{
							ID: defaultJobTargetKey.TaskRunID,
							SourcesStatus: []SourceStatus{
								{
									ID:        defaultJobTargetKey.SourceID,
									Stats:     Stats{},
									Completed: false,
									DestinationsStatus: []DestinationStatus{
										{
											ID:        defaultJobTargetKey.DestinationID,
											Completed: false,
											Stats: Stats{
												In:     8,
												Out:    6,
												Failed: 1,
											},
										},
									},
								},
							},
						},
					},
				}
				sameResults := reflect.DeepEqual(totalStatsA, totalStatsB)
				expectedResults := reflect.DeepEqual(expected, totalStatsB)
				return sameResults && expectedResults
			}, 30*time.Second, 100*time.Millisecond, "Status from both services should be same and representing the sum")
		})

		t.Run("should be able to query both services for the same jobRunId and receive same failed Records", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			jobRunId := newJobRunId()
			addFailedRecords(t, f.pgA.db, jobRunId, defaultJobTargetKey, f.serviceA, []FailedRecord{
				{Record: json.RawMessage(`{"id": "1"}`)},
				{Record: json.RawMessage(`{"id": "2"}`)},
			})
			addFailedRecords(t, f.pgB.db, jobRunId, defaultJobTargetKey, f.serviceB, []FailedRecord{
				{Record: json.RawMessage(`{"id": "2"}`)},
				{Record: json.RawMessage(`{"id": "3"}`)},
			})
			expected := JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{
					{
						ID: defaultJobTargetKey.TaskRunID,
						Sources: []SourceFailedRecords[FailedRecord]{
							{
								ID: defaultJobTargetKey.SourceID,
								Destinations: []DestinationFailedRecords[FailedRecord]{{
									ID: defaultJobTargetKey.DestinationID,
									Records: []FailedRecord{
										{Record: []byte(`{"id": "1"}`)},
										{Record: []byte(`{"id": "2"}`)},
										{Record: []byte(`{"id": "2"}`)},
										{Record: []byte(`{"id": "3"}`)},
									},
								}},
							},
						},
					},
				},
			}

			var err error
			var failedKeysA, failedKeysB JobFailedRecordsV2
			require.Eventually(t, func() bool {
				failedKeysA, err = f.serviceA.GetFailedRecords(t.Context(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = f.serviceB.GetFailedRecords(t.Context(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceB: %w", err)
					return false
				}
				if !reflect.DeepEqual(failedKeysA, failedKeysB) {
					err = fmt.Errorf("failed keys from serviceA are different compared to failed keys from serviceB")
					return false
				}
				if len(failedKeysA.Tasks) != 1 || len(failedKeysA.Tasks[0].Sources) != 1 || len(failedKeysA.Tasks[0].Sources[0].Destinations) != 1 {
					err = fmt.Errorf("failed keys from serviceA don't contain 1 task with 1 source and 1 destination")
					return false
				}
				sort.Slice(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records, func(i, j int) bool {
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i].Record) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j].Record)
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, 30*time.Second, 100*time.Millisecond, "Failed Records from both services should be the same", string(mustMarshal(failedKeysA)), string(mustMarshal(failedKeysB)), string(mustMarshal(expected)), err)
		})

		t.Run("should be able to create the same services again and not affect publications and subscriptions", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			createService(t, f.configA)
			createService(t, f.configB)
		})

		t.Run("should recreate a subscription whose replication slot was deleted on the publisher", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			ctx := t.Context()
			subscriptionName := truncateIdentifier(
				replSlotDisallowedChars.ReplaceAllString(strings.ToLower(f.configA.LocalHostname), "_") + "_rsources_stats_sub",
			)

			// Sanity: the slot exists on the publisher (pgA) after the initial setup.
			require.True(t, replicationSlotExists(t, ctx, f.pgA.db, subscriptionName), "slot should exist after initial setup")

			// Simulate slot loss on the publisher while the subscription survives on the shared db:
			// disable the subscription so the walsender releases the slot, then drop the slot.
			_, err := f.pgC.db.ExecContext(ctx, fmt.Sprintf(`ALTER SUBSCRIPTION %q DISABLE`, subscriptionName))
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				_, err := f.pgA.db.ExecContext(ctx, `SELECT pg_drop_replication_slot($1)`, subscriptionName)
				return err == nil
			}, 30*time.Second, 100*time.Millisecond, "should be able to drop the slot once it becomes inactive")
			require.False(t, replicationSlotExists(t, ctx, f.pgA.db, subscriptionName))

			// Re-running setup must detect the stale subscription, drop it and recreate it with a fresh slot.
			f.serviceA = createService(t, f.configA)
			require.True(t, replicationSlotExists(t, ctx, f.pgA.db, subscriptionName), "slot should be recreated")

			// Replication should resume end-to-end: a stat written to the local db must reach the shared db.
			jobRunId := newJobRunId()
			increment(t, f.pgA.db, jobRunId, defaultJobTargetKey, Stats{In: 5, Out: 4, Failed: 1}, f.serviceA)
			require.Eventually(t, func() bool {
				status, err := f.serviceA.GetStatus(ctx, jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				return err == nil && len(status.TasksStatus) == 1
			}, 30*time.Second, 100*time.Millisecond, "replication should resume after slot recreation")
		})

		t.Run("should cleanup orphaned rows in the shared db older than the shared retention", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			ctx := t.Context()
			jobRunId := newJobRunId()
			oldTs := time.Now().Add(-time.Duration(defaultRetentionPeriodInHours+defaultSharedRetentionBufferPeriodInHours+1) * time.Hour)

			insertOrphanRows(t, ctx, f.pgC.db, jobRunId, oldTs)

			require.NoError(t, f.serviceA.(*sourcesHandler).doCleanupSharedTables(ctx))

			require.Equal(t, sharedRowCounts{}, countSharedRows(t, ctx, f.pgC.db, jobRunId))
		})

		t.Run("should not cleanup shared db rows that are within the shared retention buffer", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			ctx := t.Context()
			jobRunId := newJobRunId()
			// older than local retention but within the shared retention buffer
			midTs := time.Now().Add(-time.Duration(defaultRetentionPeriodInHours+1) * time.Hour)

			insertOrphanRows(t, ctx, f.pgC.db, jobRunId, midTs)

			require.NoError(t, f.serviceA.(*sourcesHandler).doCleanupSharedTables(ctx))

			require.Equal(t, sharedRowCounts{stats: 1, keys: 1, records: 1}, countSharedRows(t, ctx, f.pgC.db, jobRunId))
		})

		t.Run("should skip shared cleanup when another node holds the advisory lock", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			ctx := t.Context()
			jobRunId := newJobRunId()
			oldTs := time.Now().Add(-time.Duration(defaultRetentionPeriodInHours+defaultSharedRetentionBufferPeriodInHours+1) * time.Hour)

			insertOrphanRows(t, ctx, f.pgC.db, jobRunId, oldTs)

			holdConn, err := f.pgC.db.Conn(ctx)
			require.NoError(t, err)
			holdTx, err := holdConn.BeginTx(ctx, nil)
			require.NoError(t, err)
			_, err = holdTx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, sharedCleanupLockID)
			require.NoError(t, err)

			require.NoError(t, f.serviceA.(*sourcesHandler).doCleanupSharedTables(ctx))
			require.Equal(t, sharedRowCounts{stats: 1, keys: 1, records: 1}, countSharedRows(t, ctx, f.pgC.db, jobRunId), "rows must survive while the lock is held")

			require.NoError(t, holdTx.Rollback())
			require.NoError(t, holdConn.Close())

			require.NoError(t, f.serviceA.(*sourcesHandler).doCleanupSharedTables(ctx))
			require.Equal(t, sharedRowCounts{}, countSharedRows(t, ctx, f.pgC.db, jobRunId), "rows must be cleaned up after the lock is released")
		})

		t.Run("shouldn't be able to create a service when wal_level=logical is not set on the local db", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			postgres4Hostname := randomString() + "-4"
			pgD := newDBResource(t, f.pool, f.network.ID, postgres4Hostname)
			badConfig := JobServiceConfig{
				LocalHostname:          postgres4Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgD.externalDSN,
				SharedConn:             f.pgC.externalDSN,
				SubscriptionTargetConn: pgD.internalDSN,
				Log:                    logger.NOP,
				ShouldSetupSharedDB:    true,
			}
			sts, err := memstats.New()
			require.NoError(t, err)

			service, err := NewJobService(t.Context(), badConfig, sts)
			closeJobServiceOnCleanup(t, service)
			require.Error(t, err, "it shouldn't able to create the service")
		})

		t.Run("shouldn't be able to create a service when an invalid SubscriptionTargetConn is provided", func(t *testing.T) {
			f := newMultiTenantFixture(t)
			postgres4Hostname := randomString() + "-4"
			pgD := newDBResource(t, f.pool, f.network.ID, postgres4Hostname)
			badConfig := JobServiceConfig{
				LocalHostname:          postgres4Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgD.externalDSN,
				SharedConn:             f.pgC.externalDSN,
				SubscriptionTargetConn: pgD.externalDSN,
				Log:                    logger.NOP,
				ShouldSetupSharedDB:    true,
			}
			sts, err := memstats.New()
			require.NoError(t, err)

			service, err := NewJobService(t.Context(), badConfig, sts)
			closeJobServiceOnCleanup(t, service)
			require.Error(t, err, "it shouldn't able to create the service")
		})
	})

	t.Run("monitoring lag when shared db is configured", func(t *testing.T) {
		t.Run("should be able to monitor lag when shared db is configured", func(t *testing.T) {
			f := newMonitoringFixture(t)
			lagGauge := new(mockGauge)
			replicationSlotGauge := new(mockGauge)
			defer f.startMonitor(t, lagGauge, replicationSlotGauge)()

			require.Eventually(t, func() bool {
				return replicationSlotGauge.value() == int64(1) && lagGauge.wasGauged()
			}, 30*time.Second, 100*time.Millisecond, "should have one replication slot and some non-nil lag")
		})

		t.Run("unavailability of shared db is handled via -1 replication slot gauge", func(t *testing.T) {
			f := newMonitoringFixture(t)
			// kill shared db
			_ = f.pgB.resource.Close()

			lagGauge := new(mockGauge)
			replicationSlotGauge := new(mockGauge)
			defer f.startMonitor(t, lagGauge, replicationSlotGauge)()

			require.Eventually(t, func() bool {
				return lagGauge.value() == float64(-1)
			}, 30*time.Second, 100*time.Millisecond, "should have -1 replication slot")
		})
	})
}

func mustMarshal[T any](v T) []byte {
	b, _ := jsonrs.Marshal(v)
	return b
}

// mock Gauges
//
// Monitor gauges from its own goroutine while the Eventually poller reads, so the
// fields have to be guarded: without the mutex the two monitoring specs report a data
// race under -race.
type mockGauge struct {
	mu     sync.Mutex
	gauge  any
	gauged bool
}

func (g *mockGauge) Gauge(value any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gauge = value
	g.gauged = true
}

func (g *mockGauge) value() any {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gauge
}

func (g *mockGauge) wasGauged() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gauged
}

func createService(t testing.TB, config JobServiceConfig) JobService {
	t.Helper()
	sts, err := memstats.New()
	require.NoError(t, err)

	service, err := NewJobService(t.Context(), config, sts)
	closeJobServiceOnCleanup(t, service)
	require.NoError(t, err, "it should be able to create the service")
	return service
}

// closeJobServiceOnCleanup closes the connection pools that NewJobService opened, since JobService has no Close method.
func closeJobServiceOnCleanup(t testing.TB, service JobService) {
	t.Helper()
	sh, ok := service.(*sourcesHandler)
	if !ok || sh == nil {
		return
	}
	t.Cleanup(func() {
		_ = sh.localDB.Close()
		if sh.sharedDB != nil {
			_ = sh.sharedDB.Close()
		}
	})
}

func addFailedRecords(t testing.TB, db *sql.DB, jobRunId string, jobTargetKey JobTargetKey, sh JobService, records []FailedRecord) {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err, "it should be able to begin the transaction")
	err = sh.AddFailedRecords(t.Context(), tx, jobRunId, jobTargetKey, records)
	require.NoError(t, err, "it should be able to add failed records")
	err = tx.Commit()
	require.NoError(t, err, "it should be able to commit the transaction")
}

type sharedRowCounts struct {
	stats, keys, records int
}

func insertOrphanRows(t testing.TB, ctx context.Context, db *sql.DB, jobRunId string, ts time.Time) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO rsources_stats
		(db_name, job_run_id, task_run_id, source_id, destination_id, in_count, out_count, failed_count, ts)
		VALUES ('ghost', $1, 'task', 'source', 'destination', 1, 1, 0, $2)`,
		jobRunId, ts)
	require.NoError(t, err)

	keyId := ksuid.New().String()
	_, err = db.ExecContext(ctx, `INSERT INTO rsources_failed_keys_v2
		(id, db_name, job_run_id, task_run_id, source_id, destination_id, ts)
		VALUES ($1, 'ghost', $2, 'task', 'source', 'destination', $3)`,
		keyId, jobRunId, ts)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO rsources_failed_keys_v2_records
		(id, record_id, ts, code) VALUES ($1, 'record', $2, 0)`,
		keyId, ts)
	require.NoError(t, err)
}

func countSharedRows(t testing.TB, ctx context.Context, db *sql.DB, jobRunId string) sharedRowCounts {
	t.Helper()
	var c sharedRowCounts
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM rsources_stats WHERE job_run_id = $1`, jobRunId).Scan(&c.stats))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM rsources_failed_keys_v2 WHERE job_run_id = $1`, jobRunId).Scan(&c.keys))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM rsources_failed_keys_v2_records r
		JOIN rsources_failed_keys_v2 k ON k.id = r.id WHERE k.job_run_id = $1`, jobRunId).Scan(&c.records))
	return c
}

func replicationSlotExists(t testing.TB, ctx context.Context, db *sql.DB, slotName string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slotName).Scan(&exists))
	return exists
}

func increment(t testing.TB, db *sql.DB, jobRunId string, key JobTargetKey, stat Stats, sh JobService) {
	t.Helper()
	require.NoError(t, incrementStats(t.Context(), db, jobRunId, key, stat, sh))
}

// incrementStats increments the stats of key in a transaction of its own. It reports
// failures as errors so that it can run outside the test goroutine.
func incrementStats(ctx context.Context, db *sql.DB, jobRunId string, key JobTargetKey, stat Stats, sh JobService) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("beginning the transaction: %w", err)
	}
	if err := sh.IncrementStats(ctx, tx, jobRunId, key, stat); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("incrementing stats: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing the transaction: %w", err)
	}
	return nil
}

func newJobRunId() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

// newDockerNetwork creates a docker network that is removed once the test and all
// containers started after it have been cleaned up.
func newDockerNetwork(t testing.TB, pool *dockertest.Pool) *docker.Network {
	t.Helper()
	networkId := randomString()
	network, _ := pool.Client.NetworkInfo(networkId)
	if network == nil {
		var err error
		network, err = pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: networkId})
		require.NoError(t, err)
	}
	for containerID := range network.Containers { // Remove any containers left from previous runs
		_ = pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: containerID, Force: true, RemoveVolumes: true})
	}
	t.Cleanup(func() { _ = pool.Client.RemoveNetwork(network.ID) })
	return network
}

// newDBResource starts a postgres container that is purged when the test ends.
func newDBResource(t testing.TB, pool *dockertest.Pool, networkId, hostname string, params ...string) postgresResource {
	t.Helper()
	cmd := []string{"postgres"}
	if len(params) > 0 {
		cmd = append(cmd, "-c")
		cmd = append(cmd, params...)
	}
	username := rand.String(10)
	password := rand.String(10)
	database := rand.String(10)
	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15-alpine",
		NetworkID:  networkId,
		Hostname:   hostname,
		Env: []string{
			"POSTGRES_PASSWORD=" + password,
			"POSTGRES_DB=" + database,
			"POSTGRES_USER=" + username,
		},
		ExposedPorts: []string{"5432"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {
				{
					HostIP:   "127.0.0.1",
					HostPort: "",
				},
			},
		},
		Cmd: cmd,
	}, func(hc *docker.HostConfig) {
		hc.PublishAllPorts = false
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Purge(resource) })

	port := resource.GetPort("5432/tcp")
	externalDSN := fmt.Sprintf("postgres://%[1]s:%[2]s@127.0.0.1:%[3]s/%[4]s?sslmode=disable", username, password, port, database)
	internalDSN := fmt.Sprintf("postgres://%[1]s:%[2]s@%[3]s:5432/%[4]s?sslmode=disable", username, password, hostname, database)
	var (
		db  *sql.DB
		dsn = fmt.Sprintf("host=localhost port=%[1]s user=%[2]s password=%[3]s dbname=%[4]s sslmode=disable options='-c idle_in_transaction_session_timeout=300000' ", port, username, password, database)
	)
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	err = pool.Retry(func() (err error) {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return err
		}
		return nil
	})
	require.NoError(t, err, dsn)
	t.Cleanup(func() { _ = db.Close() })
	return postgresResource{
		db:          db,
		resource:    resource,
		internalDSN: internalDSN,
		externalDSN: externalDSN,
	}
}

func randomString() string {
	return ksuid.New().String()
}
