package rsources

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

const (
	captureJobRunID  = "job-run-1"
	captureTaskRunID = "task-run-1"
	captureSourceID  = "src-1"
	captureDestID    = "dst-1"
	captureWorkspace = "ws-1"
)

// captureHarness drives one collect/publish cycle and hands back the FailedRecords
// the collector actually asked the job service to store.
type captureHarness struct {
	collector  StatsCollector
	conf       *config.Config
	statsInMem *memstats.Store
	log        *capturingLogger
	records    []FailedRecord
}

func newCaptureHarness(t *testing.T) *captureHarness {
	t.Helper()

	statsStore, err := memstats.New()
	require.NoError(t, err)

	h := &captureHarness{conf: config.New(), statsInMem: statsStore, log: &capturingLogger{Logger: logger.NOP}}

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

	h.collector = NewStatsCollector(js, "test", statsStore, WithErrorCaptureSettings(
		h.log,
		h.conf.GetReloadableBoolVar(false, captureErrorDetailKey),
		h.conf.GetReloadableIntVar(defaultMaxErrorLength, 1, maxErrorLengthKey),
		h.conf.GetReloadableStringSliceVar(nil, blockedConnectionsKey),
		h.conf.GetReloadableStringSliceVar(nil, blockedWorkspacesKey),
	))
	return h
}

// run indexes a single opted-in-or-not rETL job, collects the given status for it and
// publishes.
func (h *captureHarness) run(t *testing.T, job *jobsdb.JobT, status *jobsdb.JobStatusT) {
	t.Helper()
	h.collector.BeginProcessing([]*jobsdb.JobT{job})
	h.collector.CollectFailedRecords([]*jobsdb.JobStatusT{status})
	require.NoError(t, h.collector.Publish(context.Background(), nil))
}

func (h *captureHarness) counter(t *testing.T, name string, tags map[string]string) int {
	t.Helper()
	m := h.statsInMem.Get(name, tags)
	if m == nil {
		return 0
	}
	return int(m.LastValue())
}

// captureJob builds an rETL job whose parameters carry the numeric record id (the
// _rudder_id rudder-sources sends) and, optionally, the per-connection capture_error
// opt-in exactly as the processor marshals it. jobID doubles as the record id so
// that records from different jobs stay distinguishable.
func captureJob(t *testing.T, jobID int64, captureError any) *jobsdb.JobT {
	t.Helper()
	params := map[string]any{
		"source_job_run_id":  captureJobRunID,
		"source_task_run_id": captureTaskRunID,
		"source_id":          captureSourceID,
		"destination_id":     captureDestID,
		"record_id":          jobID,
	}
	if captureError != nil {
		params["capture_error"] = captureError
	}
	raw, err := jsonrs.Marshal(params)
	require.NoError(t, err)
	return &jobsdb.JobT{JobID: jobID, UUID: uuid.New(), Parameters: raw, WorkspaceId: captureWorkspace}
}

func abortedStatusWithError(jobID int64, code, errorResponse string) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		JobID:         jobID,
		JobState:      jobsdb.Aborted.State,
		ErrorCode:     code,
		ErrorResponse: []byte(errorResponse),
		WorkspaceId:   captureWorkspace,
	}
}

func TestCollectFailedRecordsErrorCapture(t *testing.T) {
	t.Run("A2.1 global switch off stores the record and code but no message", func(t *testing.T) {
		h := newCaptureHarness(t)
		// capture_error is on for the connection: only the global switch is off.
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "422", `{"response":"rejected"}`))

		require.Len(t, h.records, 1)
		require.Equal(t, 422, h.records[0].Code)
		require.Equal(t, `1`, string(h.records[0].Record))
		require.Empty(t, h.records[0].ErrorResponse)
	})

	t.Run("A2.2 global on but the connection did not opt in stores no message", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.run(t, captureJob(t, 1, nil), abortedStatusWithError(1, "422", `{"response":"rejected"}`))

		require.Len(t, h.records, 1)
		require.Equal(t, 422, h.records[0].Code)
		require.Empty(t, h.records[0].ErrorResponse)

		// an explicit false is equally not an opt-in
		h2 := newCaptureHarness(t)
		h2.conf.Set(captureErrorDetailKey, true)
		h2.run(t, captureJob(t, 1, false), abortedStatusWithError(1, "422", `{"response":"rejected"}`))
		require.Len(t, h2.records, 1)
		require.Empty(t, h2.records[0].ErrorResponse)
	})

	t.Run("A2.3 global on and opted in stores the unwrapped message", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "422", `{"response":"rejected: bad email"}`))

		require.Len(t, h.records, 1)
		require.Equal(t, 422, h.records[0].Code)
		require.Equal(t, "rejected: bad email", h.records[0].ErrorResponse)

		require.Equal(t, 1, h.counter(t, rsourcesErrorCaptured, map[string]string{
			"sourceId": captureSourceID, "destinationId": captureDestID, "workspaceId": captureWorkspace,
		}))
		require.Equal(t, 0, h.counter(t, rsourcesErrorClipped, map[string]string{
			"sourceId": captureSourceID, "destinationId": captureDestID, "workspaceId": captureWorkspace,
		}))
	})

	t.Run("A2.4 a blacklisted connection keeps record and code but loses the message", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.conf.Set(blockedConnectionsKey, []string{captureSourceID + ":" + captureDestID})
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "422", `{"response":"rejected"}`))

		require.Len(t, h.records, 1)
		require.Equal(t, 422, h.records[0].Code, "counts must survive suppression")
		require.Equal(t, `1`, string(h.records[0].Record), "the retry key must survive suppression")
		require.Empty(t, h.records[0].ErrorResponse)

		require.Equal(t, 1, h.counter(t, rsourcesErrorSuppressed, map[string]string{"scope": blockedScopeConnection}))
		require.Equal(t, 0, h.counter(t, rsourcesErrorCaptured, map[string]string{
			"sourceId": captureSourceID, "destinationId": captureDestID, "workspaceId": captureWorkspace,
		}))
	})

	t.Run("A2.4 a different connection on the blacklist does not suppress this one", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.conf.Set(blockedConnectionsKey, []string{"other-src:other-dst"})
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "422", `{"response":"rejected"}`))

		require.Len(t, h.records, 1)
		require.Equal(t, "rejected", h.records[0].ErrorResponse)
	})

	t.Run("A2.5 a blacklisted workspace loses the message and keeps the counts", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.conf.Set(blockedWorkspacesKey, []string{captureWorkspace})
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "410", `{"reason":"job expired"}`))

		require.Len(t, h.records, 1)
		require.Equal(t, 410, h.records[0].Code)
		require.Empty(t, h.records[0].ErrorResponse)
		require.Equal(t, 1, h.counter(t, rsourcesErrorSuppressed, map[string]string{"scope": blockedScopeWorkspace}))
	})

	t.Run("A2.6 a blacklist edit takes effect on the next capture without a restart", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)

		// first batch: captured
		h.collector.BeginProcessing([]*jobsdb.JobT{captureJob(t, 1, true)})
		h.collector.CollectFailedRecords([]*jobsdb.JobStatusT{
			abortedStatusWithError(1, "422", `{"response":"first"}`),
		})

		// operator blacklists the connection mid-flight, no restart
		h.conf.Set(blockedConnectionsKey, []string{captureSourceID + ":" + captureDestID})

		// second batch on the same collector: suppressed
		h.collector.BeginProcessing([]*jobsdb.JobT{captureJob(t, 2, true)})
		h.collector.CollectFailedRecords([]*jobsdb.JobStatusT{
			abortedStatusWithError(2, "422", `{"response":"second"}`),
		})
		require.NoError(t, h.collector.Publish(context.Background(), nil))

		require.Len(t, h.records, 2)
		byRecord := map[string]string{}
		for _, r := range h.records {
			byRecord[string(r.Record)] = r.ErrorResponse
		}
		require.Equal(t, "first", byRecord["1"], "captured before the blacklist edit")
		require.Equal(t, "", byRecord["2"], "suppressed after the blacklist edit")
		require.Len(t, byRecord, 2)
	})

	t.Run("A1.6 an oversized message is clipped and counted", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.conf.Set(maxErrorLengthKey, 16)
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "422", `{"response":"`+strings.Repeat("x", 100)+`"}`))

		require.Len(t, h.records, 1)
		require.Len(t, h.records[0].ErrorResponse, 16)
		require.Equal(t, 1, h.counter(t, rsourcesErrorClipped, map[string]string{
			"sourceId": captureSourceID, "destinationId": captureDestID, "workspaceId": captureWorkspace,
		}))
	})

	t.Run("A1.9 non-aborted statuses are not captured at all", func(t *testing.T) {
		for _, state := range []string{jobsdb.Succeeded.State, jobsdb.Filtered.State, jobsdb.Failed.State} {
			h := newCaptureHarness(t)
			h.conf.Set(captureErrorDetailKey, true)
			h.collector.BeginProcessing([]*jobsdb.JobT{captureJob(t, 1, true)})
			h.collector.CollectFailedRecords([]*jobsdb.JobStatusT{{
				JobID:         1,
				JobState:      state,
				ErrorCode:     "200",
				ErrorResponse: []byte(`{"response":"should never be captured"}`),
				WorkspaceId:   captureWorkspace,
			}})
			require.NoError(t, h.collector.Publish(context.Background(), nil))
			require.Empty(t, h.records, "state %s must not produce a failed record", state)
		}
	})

	t.Run("A1.5 a dropped job with an empty error response stores an empty message", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "", ``))

		require.Len(t, h.records, 1)
		require.Equal(t, 0, h.records[0].Code)
		require.Empty(t, h.records[0].ErrorResponse)
	})

	t.Run("A3.5 [SEC-L1] the opt-in is honored without a backend-config cross-check", func(t *testing.T) {
		// The flag rides context.sources on the event, so a source can forge it. This
		// slice deliberately does NOT cross-check it against the connection's
		// errorDetailsConfig: a forged flag therefore CAN induce the transient jobsdb
		// capture of that source's own error text (self-scoped, bounded by the 72h
		// rsources retention). It cannot induce durable warehouse landing - that gate
		// is server-authoritative and lives in rudder-sources.
		//
		// The enforceable boundaries on this side are the global switch and the
		// operator blacklist, both asserted below. Change this test only alongside a
		// deliberate decision to add the cross-check.
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "422", `{"response":"forged capture"}`))
		require.Equal(t, "forged capture", h.records[0].ErrorResponse,
			"documented behaviour: capture gating is best effort, the landing gate is the real boundary")

		// the operator blacklist still overrides a forged flag
		blocked := newCaptureHarness(t)
		blocked.conf.Set(captureErrorDetailKey, true)
		blocked.conf.Set(blockedConnectionsKey, []string{captureSourceID + ":" + captureDestID})
		blocked.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "422", `{"response":"forged capture"}`))
		require.Empty(t, blocked.records[0].ErrorResponse)

		// and so does the global switch
		off := newCaptureHarness(t)
		off.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "422", `{"response":"forged capture"}`))
		require.Empty(t, off.records[0].ErrorResponse)
	})

	t.Run("telemetry never carries the message body", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		secret := "Invalid API key: sk-live-abcdef"
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "401", `{"response":"`+secret+`"}`))

		require.Equal(t, secret, h.records[0].ErrorResponse)
		for _, m := range h.statsInMem.GetAll() {
			for _, v := range m.Tags {
				require.NotContains(t, v, "sk-live", "metric tag leaked the message body: %s=%s", m.Name, v)
			}
		}
	})
}

// TestDefaultParametersParserCaptureError pins how the collector reads the opt-in out
// of the job parameters, including the rejected string shape (test plan A3.4).
func TestDefaultParametersParserCaptureError(t *testing.T) {
	parse := func(params string) jobParameters {
		return defaultParametersParser([]byte(params))
	}

	t.Run("a real json bool enables capture", func(t *testing.T) {
		p := parse(`{"source_job_run_id":"jr","source_id":"s","destination_id":"d","record_id":1,"capture_error":true}`)
		require.True(t, p.captureError)
		require.Equal(t, "jr", p.jobRunID)
		require.Equal(t, "1", p.recordID)
		require.Equal(t, "s", p.target.SourceID)
		require.Equal(t, "d", p.target.DestinationID)
	})

	t.Run("A3.4 the string \"true\" is a rejected shape", func(t *testing.T) {
		require.False(t, parse(`{"source_job_run_id":"jr","capture_error":"true"}`).captureError)
		require.False(t, parse(`{"source_job_run_id":"jr","capture_error":1}`).captureError)
		require.False(t, parse(`{"source_job_run_id":"jr","capture_error":"1"}`).captureError)
	})

	t.Run("false and absent are both off", func(t *testing.T) {
		require.False(t, parse(`{"source_job_run_id":"jr","capture_error":false}`).captureError)
		require.False(t, parse(`{"source_job_run_id":"jr"}`).captureError)
	})

	t.Run("the opt-in is found even when it is the last key", func(t *testing.T) {
		// The early-exit counter must account for capture_error, otherwise the
		// ForEach stops before reaching it.
		p := parse(`{"source_job_run_id":"jr","source_task_run_id":"tr","source_id":"s","destination_id":"d","record_id":1,"other":"x","capture_error":true}`)
		require.True(t, p.captureError)
	})

	t.Run("IgnoreDestinationID still blanks the destination", func(t *testing.T) {
		sc := newStatsCollector(nil, "test", memstatsOrFail(t), IgnoreDestinationID())
		p := sc.parametersParser([]byte(`{"source_job_run_id":"jr","source_id":"s","destination_id":"d","capture_error":true}`))
		require.Empty(t, p.target.DestinationID)
		require.Equal(t, "s", p.target.SourceID)
		require.True(t, p.captureError)
	})
}

// TestFailedRecordWireShape pins the internal v2 API leaf: {record, code, error}.
func TestFailedRecordWireShape(t *testing.T) {
	t.Run("A4.1 the error field rides alongside record and code", func(t *testing.T) {
		b, err := jsonrs.Marshal(FailedRecord{Record: []byte(`"rec-1"`), Code: 422, ErrorResponse: "rejected"})
		require.NoError(t, err)
		require.Equal(t, `"rec-1"`, gjson.GetBytes(b, "record").Raw)
		require.Equal(t, int64(422), gjson.GetBytes(b, "code").Int())
		require.Equal(t, "rejected", gjson.GetBytes(b, "error").String())
	})

	t.Run("A4.3 an old client ignores the new field", func(t *testing.T) {
		type oldFailedRecord struct {
			Record json.RawMessage `json:"record"`
			Code   int             `json:"code"`
		}
		var old oldFailedRecord
		require.NoError(t, jsonrs.Unmarshal([]byte(`{"record":"rec-1","code":422,"error":"rejected"}`), &old))
		require.Equal(t, 422, old.Code)
	})

	t.Run("A4.4 a new client tolerates a server that does not send the field", func(t *testing.T) {
		var rec FailedRecord
		require.NoError(t, jsonrs.Unmarshal([]byte(`{"record":"rec-1","code":422}`), &rec))
		require.Equal(t, 422, rec.Code)
		require.Empty(t, rec.ErrorResponse)
	})

	t.Run("the field is omitted entirely when capture is off", func(t *testing.T) {
		b, err := jsonrs.Marshal(FailedRecord{Record: []byte(`"rec-1"`), Code: 422})
		require.NoError(t, err)
		require.False(t, gjson.GetBytes(b, "error").Exists())
		require.Equal(t, `{"record":"rec-1","code":422}`, string(b))
	})
}

func memstatsOrFail(t *testing.T) *memstats.Store {
	t.Helper()
	s, err := memstats.New()
	require.NoError(t, err)
	return s
}

// TestErrorCaptureTelemetry pins the per-publish capture summary: emitted once per
// connection, carrying counts and identifiers, never the message body.
func TestErrorCaptureTelemetry(t *testing.T) {
	t.Run("the summary reports captured, clipped and suppressed counts", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.conf.Set(maxErrorLengthKey, 8)

		h.collector.BeginProcessing([]*jobsdb.JobT{
			captureJob(t, 1, true), captureJob(t, 2, true), captureJob(t, 3, true),
		})
		h.collector.CollectFailedRecords([]*jobsdb.JobStatusT{
			abortedStatusWithError(1, "422", `{"response":"short"}`),
			abortedStatusWithError(2, "422", `{"response":"`+strings.Repeat("x", 100)+`"}`),
			abortedStatusWithError(3, "422", `{"response":"also long enough to clip"}`),
		})
		require.NoError(t, h.collector.Publish(context.Background(), nil))

		summaries := h.log.withMessage(h.log.infos, "rsources: error capture summary")
		require.Len(t, summaries, 1, "exactly one summary per connection per publish")
		fields := summaries[0].fieldMap()
		require.Equal(t, captureSourceID, fields["sourceId"])
		require.Equal(t, captureDestID, fields["destinationId"])
		require.Equal(t, captureWorkspace, fields["workspaceId"])
		require.Equal(t, captureJobRunID, fields["job_run_id"])
		require.Equal(t, int64(3), fields["capturedCount"])
		require.Equal(t, int64(2), fields["clippedCount"])
		require.Equal(t, int64(0), fields["suppressedByBlacklist"])
		require.Empty(t, h.log.withMessage(h.log.warns, "rsources: error capture blocked"))
	})

	t.Run("suppression is warned once with the scope that matched", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		h.conf.Set(blockedConnectionsKey, []string{captureSourceID + ":" + captureDestID})

		h.collector.BeginProcessing([]*jobsdb.JobT{captureJob(t, 1, true), captureJob(t, 2, true)})
		h.collector.CollectFailedRecords([]*jobsdb.JobStatusT{
			abortedStatusWithError(1, "422", `{"response":"rejected"}`),
			abortedStatusWithError(2, "422", `{"response":"rejected"}`),
		})
		require.NoError(t, h.collector.Publish(context.Background(), nil))

		warns := h.log.withMessage(h.log.warns, "rsources: error capture blocked")
		require.Len(t, warns, 1, "one warning per connection per publish, not one per record")
		require.Equal(t, blockedScopeConnection, warns[0].fieldMap()["reason"])

		summary := h.log.withMessage(h.log.infos, "rsources: error capture summary")[0].fieldMap()
		require.Equal(t, int64(2), summary["suppressedByBlacklist"])
		require.Equal(t, int64(0), summary["capturedCount"])
	})

	t.Run("no log field ever carries the message body", func(t *testing.T) {
		h := newCaptureHarness(t)
		h.conf.Set(captureErrorDetailKey, true)
		secret := "Invalid API key: sk-live-abcdef"
		h.run(t, captureJob(t, 1, true), abortedStatusWithError(1, "401", `{"response":"`+secret+`"}`))

		require.Equal(t, secret, h.records[0].ErrorResponse, "the message must still reach storage")
		require.NotEmpty(t, h.log.infos, "the summary must have been emitted")
		for _, entry := range append(h.log.infos, h.log.warns...) {
			require.NotContains(t, entry.msg, "sk-live")
			for name, v := range entry.fieldMap() {
				require.NotContains(t, fmt.Sprint(v), "sk-live",
					"log field %q leaked the message body", name)
			}
		}
	})
}

type capturedLog struct {
	msg    string
	fields []logger.Field
}

func (c capturedLog) fieldMap() map[string]any {
	m := make(map[string]any, len(c.fields))
	for _, f := range c.fields {
		m[f.Name()] = f.Value()
	}
	return m
}

// capturingLogger records the structured log lines the collector emits so tests can
// assert on both their contents and, crucially, what they must never contain.
type capturingLogger struct {
	logger.Logger
	infos []capturedLog
	warns []capturedLog
}

func (l *capturingLogger) Infon(msg string, fields ...logger.Field) {
	l.infos = append(l.infos, capturedLog{msg: msg, fields: fields})
}

func (l *capturingLogger) Warnn(msg string, fields ...logger.Field) {
	l.warns = append(l.warns, capturedLog{msg: msg, fields: fields})
}

func (l *capturingLogger) Child(string) logger.Logger { return l }

func (*capturingLogger) withMessage(entries []capturedLog, msg string) []capturedLog {
	var out []capturedLog
	for _, e := range entries {
		if e.msg == msg {
			out = append(out, e)
		}
	}
	return out
}
