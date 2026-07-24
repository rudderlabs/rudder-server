package activationrecords

import (
	"encoding/hex"
	"testing"

	"github.com/segmentio/go-hll"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// singularEventBatch mirrors the gateway's on-the-wire envelope
// (gateway/handle.go:516-521). The MAR reporter reads the activation fields
// back out of the EventPayload this struct serializes to, so the JSON tags and
// nesting MUST match the gateway exactly — a drift would make the read path
// silently return "" and undercount records with no runtime error.
type singularEventBatch struct {
	Batch      []map[string]any `json:"batch"`
	RequestIP  string           `json:"requestIP"`
	WriteKey   string           `json:"writeKey"`
	ReceivedAt string           `json:"receivedAt"`
}

// buildGatewayPayload serializes one event into the exact gateway EventPayload
// shape using the same marshaller the gateway uses (jsonrs). The activation
// fingerprint/origin live at batch[0].context.activation.* (mirror
// gateway/handle.go:516-532). activation is the inner map so individual keys
// can be omitted to exercise fail-closed paths.
func buildGatewayPayload(t *testing.T, activation map[string]any) []byte {
	t.Helper()
	event := map[string]any{
		"context": map[string]any{
			"activation": activation,
		},
	}
	payload, err := jsonrs.Marshal(singularEventBatch{
		Batch:      []map[string]any{event},
		RequestIP:  "10.0.0.1",
		WriteKey:   "write-key-xyz",
		ReceivedAt: "2026-06-22T00:00:00.000Z",
	})
	require.NoError(t, err)
	return payload
}

// buildGatewayParams serializes job Parameters the way the gateway does
// (mirror gateway/handle.go:484-493): an rETL record job carries both source_id
// and destination_id, and source_category=warehouse (the reverse-ETL category).
func buildGatewayParams(t *testing.T) []byte {
	t.Helper()
	params := map[string]any{
		"source_id":          "src-1",
		"destination_id":     "dst-1",
		"source_job_run_id":  "",
		"source_task_run_id": "",
		"traceparent":        "",
		"source_category":    "warehouse",
	}
	out, err := jsonrs.Marshal(params)
	require.NoError(t, err)
	return out
}

func newWireCompatReporter(t *testing.T) *UniqueActivationRecordsReporter {
	t.Helper()
	reporter, err := NewUniqueActivationRecordsReporter(logger.NOP, config.Default, stats.NOP)
	require.NoError(t, err)
	return reporter
}

// TestWireCompat proves the READ/CONSUMER side of the end-to-end wire contract
// that the records_reporter unit tests do NOT cover: that the reporter resolves
// the activation grain straight out of the REAL gateway serialization (a full
// SingularEventBatch envelope marshalled with jsonrs, not the minimal hand-written
// JSON the reporter unit tests use), and that the fingerprint HLL survives the hex
// byte round-trip the reporting backend decodes. Grain derivation rules and the
// fail-closed skip paths are covered by TestUniqueActivationRecordsReporter.
func TestWireCompat(t *testing.T) {
	reporter := newWireCompatReporter(t)
	job := &jobsdb.JobT{
		WorkspaceId: "ws-1",
		Parameters:  buildGatewayParams(t),
		EventPayload: buildGatewayPayload(t, map[string]any{
			"fingerprint": "fp-abc",
			"origin":      "data-graph-audience",
		}),
	}

	reports := reporter.GenerateReportsFromJobs([]*jobsdb.JobT{job}, map[string]SourceMetadata{
		"src-1": {Category: "warehouse", Name: "snowflake"},
	})
	require.Len(t, reports, 1)

	// The reporter must resolve the full grain from the gateway's actual on-the-wire
	// bytes: workspace from the job, source/destination from Parameters, and
	// fingerprint/origin from batch[0].context.activation in the EventPayload (the
	// read uses bracket notation "[0]" — a bare "0" would read "" and undercount).
	// The reporter unit tests assert this against hand-written JSON; here it is the
	// real jsonrs envelope, so a serialization drift is caught.
	require.Equal(t, "ws-1", reports[0].WorkspaceID)
	require.Equal(t, "src-1", reports[0].SourceID)
	require.Equal(t, "dst-1", reports[0].DestinationID)
	require.Equal(t, "data-graph-audience", reports[0].Origin)

	// HLL wire-compat: encode via the production path (ReportActivationRecords stores
	// hllToString(hll), which hex-encodes hll.ToBytes()), then decode the raw bytes
	// the way the backend does and prove a clean round-trip of exactly what lands in
	// postgres.
	encoded, err := reporter.hllToString(reports[0].FingerprintHll)
	require.NoError(t, err)

	decoded, err := hex.DecodeString(encoded)
	require.NoError(t, err)

	// rudderstack-reporting's postgres `hll` extension decodes these bytes with
	// Log2m=16, Regwidth=5. Changing Log2m/Regwidth breaks cross-service wire-compat:
	// the backend would mis-decode the header or refuse to merge the sketch — silent
	// corruption, not a crash. Keep these two values character-for-character
	// identical to the reporter's hllSettings.
	roundTripped, err := hll.FromBytes(decoded)
	require.NoError(t, err)
	require.Equal(t, 16, roundTripped.Settings().Log2m)
	require.Equal(t, 5, roundTripped.Settings().Regwidth)
	require.Equal(t, uint64(1), roundTripped.Cardinality())
}
