package rsources

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// The wiring between the failed-records collector and the SyncSettingDelegate. The
// collector holds no capture policy of its own, so everything asserted here is about
// the seam: what reaches the delegate, what comes back, and what happens when the
// delegate fails. The policy itself is tested in sync_setting_delegate_test.go.

// recordingDelegate answers with a canned value and remembers every call, so that
// "this status never reached the delegate" is an assertion on a recorded list rather
// than on the absence of an effect.
type recordingDelegate struct {
	mu       sync.Mutex
	calls    []recordedCall
	response string
	err      error
	// failAfter, when non-zero, answers "resolved" until the failAfter'th call and
	// fails from there on - a failure landing in the middle of a batch.
	failAfter int
}

type recordedCall struct {
	key    statKey
	status jobsdb.JobStatusT
}

func (d *recordingDelegate) GetErrorResponse(_ context.Context, key statKey, status *jobsdb.JobStatusT) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, recordedCall{key: key, status: *status})
	if d.failAfter > 0 && len(d.calls) < d.failAfter {
		return "resolved", nil
	}
	return d.response, d.err
}

func (d *recordingDelegate) recorded() []recordedCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]recordedCall(nil), d.calls...)
}

// collectorHarness drives one collect/publish cycle and hands back the FailedRecords
// the collector actually asked the job service to store.
type collectorHarness struct {
	collector StatsCollector
	delegate  *recordingDelegate
	records   []FailedRecord
}

func newCollectorHarness(t *testing.T, delegate *recordingDelegate) *collectorHarness {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)

	h := &collectorHarness{delegate: delegate}
	js := NewMockJobService(gomock.NewController(t))
	js.EXPECT().
		AddFailedRecords(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ any, _ string, _ JobTargetKey, records []FailedRecord) error {
			h.records = append(h.records, records...)
			return nil
		}).
		AnyTimes()
	js.EXPECT().IncrementStats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	h.collector = NewStatsCollector(js, "test", statsStore, WithSyncSettingDelegate(delegate))
	return h
}

// retlJob builds an rETL job whose parameters carry the numeric record id (the
// _rudder_id rudder-sources sends). jobID doubles as the record id so that records
// from different jobs stay distinguishable. A withRecordID of false produces the
// shape a non-rETL-record job has: tracked for stats, but with nothing to fail.
func retlJob(t *testing.T, jobID int64, withRecordID bool) *jobsdb.JobT {
	t.Helper()
	params := map[string]any{
		"source_job_run_id":  testJobRunID,
		"source_task_run_id": testTaskRunID,
		"source_id":          testSourceID,
		"destination_id":     testDestID,
	}
	if withRecordID {
		params["record_id"] = jobID
	}
	raw, err := jsonrs.Marshal(params)
	require.NoError(t, err)
	return &jobsdb.JobT{JobID: jobID, UUID: uuid.New(), Parameters: raw, WorkspaceId: testWorkspace}
}

func statusFor(jobID int64, state, code, errorResponse string) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		JobID:         jobID,
		JobState:      state,
		ErrorCode:     code,
		ErrorResponse: []byte(errorResponse),
		WorkspaceId:   testWorkspace,
	}
}

// TestCollectFailedRecordsUsesTheDelegateVerbatim pins that the stored error text is
// exactly what the delegate returned - the collector neither unwraps, clips nor
// sanitises anything of its own.
func TestCollectFailedRecordsUsesTheDelegateVerbatim(t *testing.T) {
	// Values the collector would have mangled if it still held any capture policy: a
	// value longer than defaultMaxErrorLength, a raw envelope, and a value that is not
	// an unwrapped message at all.
	responses := map[string]string{
		"a plain message":                    "rejected: bad email",
		"an empty answer":                    "",
		"a value well past the default cap":  strings.Repeat("x", defaultMaxErrorLength*2),
		"a raw envelope the delegate chose":  `{"response":"stored as is"}`,
		"bytes postgres would have rejected": "a\x00b",
	}
	for name, response := range responses {
		t.Run(name, func(t *testing.T) {
			delegate := &recordingDelegate{response: response}
			h := newCollectorHarness(t, delegate)

			h.collector.BeginProcessing([]*jobsdb.JobT{retlJob(t, 1, true)})
			require.NoError(t, h.collector.CollectFailedRecords(context.Background(),
				[]*jobsdb.JobStatusT{statusFor(1, jobsdb.Aborted.State, "422", `{"response":"ignored by the harness"}`)}))
			require.NoError(t, h.collector.Publish(context.Background(), nil))

			require.Len(t, h.records, 1)
			require.Equal(t, response, h.records[0].ErrorResponse,
				"the collector must store the delegate's answer byte for byte")
			require.Equal(t, 422, h.records[0].Code, "the code is read off the status, not the delegate")
			require.Equal(t, `1`, string(h.records[0].Record))
		})
	}

	t.Run("the delegate sees the statKey and the whole status", func(t *testing.T) {
		delegate := &recordingDelegate{response: "ok"}
		h := newCollectorHarness(t, delegate)
		h.collector.BeginProcessing([]*jobsdb.JobT{retlJob(t, 7, true)})
		status := statusFor(7, jobsdb.Aborted.State, "410", `{"reason":"job expired"}`)
		require.NoError(t, h.collector.CollectFailedRecords(context.Background(), []*jobsdb.JobStatusT{status}))

		calls := delegate.recorded()
		require.Len(t, calls, 1)
		require.Equal(t, testJobRunID, calls[0].key.jobRunId)
		require.Equal(t, testTaskRunID, calls[0].key.TaskRunID)
		require.Equal(t, testSourceID, calls[0].key.SourceID)
		require.Equal(t, testDestID, calls[0].key.DestinationID)
		require.Equal(t, testWorkspace, calls[0].status.WorkspaceId,
			"the workspace id rides on the status and the delegate needs it for the blacklist")
		require.JSONEq(t, `{"reason":"job expired"}`, string(calls[0].status.ErrorResponse))
	})
}

// TestCollectFailedRecordsPropagatesDelegateErrors pins the strict propagation
// contract: the batch is aborted rather than published with a silently empty message.
func TestCollectFailedRecordsPropagatesDelegateErrors(t *testing.T) {
	sentinel := errors.New("the pinned decision could not be read")

	t.Run("the error is wrapped, identifies the job, and appends nothing", func(t *testing.T) {
		delegate := &recordingDelegate{err: sentinel}
		h := newCollectorHarness(t, delegate)
		h.collector.BeginProcessing([]*jobsdb.JobT{retlJob(t, 42, true)})

		err := h.collector.CollectFailedRecords(context.Background(),
			[]*jobsdb.JobStatusT{statusFor(42, jobsdb.Aborted.State, "422", `{"response":"rejected"}`)})

		require.Error(t, err)
		require.ErrorIs(t, err, sentinel, "the delegate's error must stay unwrappable")
		require.ErrorContains(t, err, "42", "the message must identify the job that could not be resolved")
		require.ErrorContains(t, err, "resolving the error response")

		sc := h.collector.(*statsCollector)
		require.Empty(t, sc.failedRecordsIndex, "an unresolved record must not be queued for publishing")

		// Publishing after the failure must not store anything either.
		require.NoError(t, h.collector.Publish(context.Background(), nil))
		require.Empty(t, h.records)
	})

	t.Run("the failure stops the batch at the first unresolved record", func(t *testing.T) {
		// The collector aborts on the first error rather than resolving the rest of
		// the batch, so the delegate is called once even though three statuses were
		// handed in. Records already resolved before the failure stay in the index:
		// the contract is that the caller must not publish a collector whose
		// CollectFailedRecords returned an error.
		delegate := &recordingDelegate{failAfter: 2, err: sentinel}
		statsStore, err := memstats.New()
		require.NoError(t, err)
		js := NewMockJobService(gomock.NewController(t))
		js.EXPECT().IncrementStats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).AnyTimes()
		sc := newStatsCollector(js, "test", statsStore, WithSyncSettingDelegate(delegate))

		sc.BeginProcessing([]*jobsdb.JobT{retlJob(t, 1, true), retlJob(t, 2, true), retlJob(t, 3, true)})
		err = sc.CollectFailedRecords(context.Background(), []*jobsdb.JobStatusT{
			statusFor(1, jobsdb.Aborted.State, "422", `{"response":"one"}`),
			statusFor(2, jobsdb.Aborted.State, "422", `{"response":"two"}`),
			statusFor(3, jobsdb.Aborted.State, "422", `{"response":"three"}`),
		})

		require.ErrorIs(t, err, sentinel)
		require.ErrorContains(t, err, "job 2")
		require.Len(t, delegate.recorded(), 2, "the batch must stop at the failure, not carry on")
	})
}

// TestCollectFailedRecordsDelegateSkipsStatusesThatCannotProduceARecord pins that the
// delegate - which may go to the database - is never consulted for a status that could
// never become a failed record.
func TestCollectFailedRecordsDelegateSkipsStatusesThatCannotProduceARecord(t *testing.T) {
	t.Run("only aborted statuses reach the delegate", func(t *testing.T) {
		for _, state := range []string{
			jobsdb.Succeeded.State, jobsdb.Filtered.State, jobsdb.Failed.State,
			jobsdb.Waiting.State, jobsdb.Executing.State,
		} {
			t.Run(state, func(t *testing.T) {
				delegate := &recordingDelegate{response: "must never be stored"}
				h := newCollectorHarness(t, delegate)
				h.collector.BeginProcessing([]*jobsdb.JobT{retlJob(t, 1, true)})
				require.NoError(t, h.collector.CollectFailedRecords(context.Background(),
					[]*jobsdb.JobStatusT{statusFor(1, state, "200", `{"response":"should never be captured"}`)}))
				require.NoError(t, h.collector.Publish(context.Background(), nil))

				require.Empty(t, delegate.recorded(), "state %s must not reach the delegate", state)
				require.Empty(t, h.records, "state %s must not produce a failed record", state)
			})
		}
	})

	t.Run("a job with no record id never reaches the delegate", func(t *testing.T) {
		delegate := &recordingDelegate{response: "must never be stored"}
		h := newCollectorHarness(t, delegate)
		// job 1 has a record id, job 2 does not: both are aborted.
		h.collector.BeginProcessing([]*jobsdb.JobT{retlJob(t, 1, true), retlJob(t, 2, false)})
		require.NoError(t, h.collector.CollectFailedRecords(context.Background(), []*jobsdb.JobStatusT{
			statusFor(1, jobsdb.Aborted.State, "422", `{"response":"kept"}`),
			statusFor(2, jobsdb.Aborted.State, "422", `{"response":"no record id"}`),
		}))
		require.NoError(t, h.collector.Publish(context.Background(), nil))

		calls := delegate.recorded()
		require.Len(t, calls, 1, "only the job carrying a record id may be resolved")
		require.Equal(t, int64(1), calls[0].status.JobID)
		require.Len(t, h.records, 1)
	})

	t.Run("a status for a job that was never indexed never reaches the delegate", func(t *testing.T) {
		delegate := &recordingDelegate{response: "must never be stored"}
		h := newCollectorHarness(t, delegate)
		h.collector.BeginProcessing([]*jobsdb.JobT{retlJob(t, 1, true)})
		require.NoError(t, h.collector.CollectFailedRecords(context.Background(), []*jobsdb.JobStatusT{
			statusFor(999, jobsdb.Aborted.State, "422", `{"response":"unknown job"}`),
		}))
		require.Empty(t, delegate.recorded())
		require.NoError(t, h.collector.Publish(context.Background(), nil))
		require.Empty(t, h.records)
	})

	t.Run("a batch with no rETL jobs at all never reaches the delegate", func(t *testing.T) {
		delegate := &recordingDelegate{err: errors.New("must never be called")}
		h := newCollectorHarness(t, delegate)
		// No source_job_run_id: nothing is indexed, so the whole call short-circuits.
		h.collector.BeginProcessing([]*jobsdb.JobT{{JobID: 1, UUID: uuid.New(), Parameters: []byte(`{}`)}})
		require.NoError(t, h.collector.CollectFailedRecords(context.Background(),
			[]*jobsdb.JobStatusT{statusFor(1, jobsdb.Aborted.State, "422", `{"response":"x"}`)}),
			"an empty index must not be an error, and must not consult the delegate")
		require.Empty(t, delegate.recorded())
	})
}

// TestCollectFailedRecordsDelegateRequiresBeginProcessing pins the existing precondition,
// which the new signature must not have turned into a returned error.
func TestCollectFailedRecordsDelegateRequiresBeginProcessing(t *testing.T) {
	h := newCollectorHarness(t, &recordingDelegate{})
	require.Panics(t, func() {
		_ = h.collector.CollectFailedRecords(context.Background(), []*jobsdb.JobStatusT{
			statusFor(1, jobsdb.Aborted.State, "422", `{"response":"x"}`),
		})
	})
}

// TestCollectFailedRecordsWithoutADelegateFailsLoud pins the safety net behind the
// optional delegate.
//
// WithSyncSettingDelegate is an option, so forgetting it is a compilable mistake. The
// gateway's and the processor's collectors legitimately never pass one, which is why it
// has to stay optional - but a collector that never got one and then DOES reach
// CollectFailedRecords must fail, loudly, on the very first aborted rETL record.
// Answering "" instead would publish durable failed records with a silently empty
// error_response, indistinguishable from "the destination said nothing".
func TestCollectFailedRecordsWithoutADelegateFailsLoud(t *testing.T) {
	statsStore, err := memstats.New()
	require.NoError(t, err)
	js := NewMockJobService(gomock.NewController(t))
	js.EXPECT().AddFailedRecords(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0) // nothing may reach the job service

	// Built exactly the way the gateway and the processor build theirs.
	collector := NewStatsCollector(js, "gw", statsStore)
	job := retlJob(t, 1, true)
	collector.BeginProcessing([]*jobsdb.JobT{job})

	err = collector.CollectFailedRecords(context.Background(), []*jobsdb.JobStatusT{
		statusFor(1, jobsdb.Aborted.State, "422", `{"response":"boom"}`),
	})

	require.Error(t, err, "a delegate-less collector must not silently drop the error text")
	require.ErrorContains(t, err, `the "gw" stats collector was built without a sync setting delegate`,
		"the error must name the component so the miswiring is obvious")
	require.ErrorContains(t, err, "job 1", "the error must name the record it failed on")

	// The failure is an error, not a panic, and not a partial publish.
	require.Empty(t, collector.(*statsCollector).failedRecordsIndex,
		"no record may be collected once the delegate has failed")
}

// TestNewStatsCollectorDelegateDefaultIsOverridable pins that the fail-loud default is
// exactly that - a default - and that a nil option can never reinstate a silent nil.
func TestNewStatsCollectorDelegateDefaultIsOverridable(t *testing.T) {
	statsStore, err := memstats.New()
	require.NoError(t, err)
	js := NewMockJobService(gomock.NewController(t))

	t.Run("the default is the unsupported delegate", func(t *testing.T) {
		for _, sc := range []*statsCollector{
			newStatsCollector(js, "test", statsStore),
			newStatsCollector(js, "dropped", statsStore, IgnoreDestinationID()),
		} {
			_, err := sc.syncSettings.GetErrorResponse(context.Background(), statKey{}, &jobsdb.JobStatusT{})
			require.Error(t, err)
		}
	})

	t.Run("WithSyncSettingDelegate replaces it", func(t *testing.T) {
		sc := newStatsCollector(js, "test", statsStore, WithSyncSettingDelegate(NewStaticSyncSettingDelegate("kept", nil)))
		got, err := sc.syncSettings.GetErrorResponse(context.Background(), statKey{}, &jobsdb.JobStatusT{})
		require.NoError(t, err)
		require.Equal(t, "kept", got)
	})

	t.Run("a nil delegate is refused, leaving the fail-loud default in place", func(t *testing.T) {
		sc := newStatsCollector(js, "test", statsStore, WithSyncSettingDelegate(nil))
		require.NotNil(t, sc.syncSettings)
		_, err := sc.syncSettings.GetErrorResponse(context.Background(), statKey{}, &jobsdb.JobStatusT{})
		require.Error(t, err)
	})
}

// TestDefaultParametersParser pins how the collector reads the job parameters it
// needs. The capture opt-in used to live here; it now lives on the connection, read by
// the delegate's own backend-config subscription.
func TestDefaultParametersParser(t *testing.T) {
	t.Run("the rETL identifiers are read off the parameters", func(t *testing.T) {
		p := defaultParametersParser([]byte(
			`{"source_job_run_id":"jr","source_task_run_id":"tr","source_id":"s","destination_id":"d","record_id":1}`))
		require.Equal(t, "jr", p.jobRunID)
		require.Equal(t, "tr", p.target.TaskRunID)
		require.Equal(t, "s", p.target.SourceID)
		require.Equal(t, "d", p.target.DestinationID)
		require.Equal(t, "1", p.recordID)
	})

	t.Run("a job carrying no capture opt-in parses exactly as before", func(t *testing.T) {
		// The parameters no longer carry the flag; an old job that still does must be
		// parsed without incident and without meaning.
		p := defaultParametersParser([]byte(
			`{"source_job_run_id":"jr","source_id":"s","destination_id":"d","record_id":1,"capture_error":true}`))
		require.Equal(t, "jr", p.jobRunID)
		require.Equal(t, "1", p.recordID)
	})

	t.Run("IgnoreDestinationID blanks the destination", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)
		sc := newStatsCollector(nil, "test", statsStore, IgnoreDestinationID())
		p := sc.parametersParser([]byte(`{"source_job_run_id":"jr","source_id":"s","destination_id":"d"}`))
		require.Empty(t, p.target.DestinationID)
		require.Equal(t, "s", p.target.SourceID)
	})
}
