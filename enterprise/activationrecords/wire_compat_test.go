package activationrecords

import (
	"encoding/hex"
	"testing"

	"github.com/segmentio/go-hll"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonparser"
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
// and destination_id.
func buildGatewayParams(t *testing.T) []byte {
	t.Helper()
	params := map[string]any{
		"source_id":          "src-1",
		"destination_id":     "dst-1",
		"source_job_run_id":  "",
		"source_task_run_id": "",
		"traceparent":        "",
		"source_category":    "",
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

// TestWireCompat proves the parts of the READ/CONSUMER side of the end-to-end
// wire contract that the records_reporter unit tests do NOT cover: reading the
// activation fields straight out of the gateway-serialized payload, and the HLL
// byte round-trip under the params the reporting backend uses. Grain derivation
// and the fail-closed skip paths are covered by TestUniqueActivationRecordsReporter.
func TestWireCompat(t *testing.T) {
	// 1) jsonparser read-path — THE CORE WIRE TEST. The processor stage reads the
	// fingerprint/origin straight out of the gateway-serialized EventPayload with
	// go-kit jsonparser. If this path drifts, every record undercounts silently.
	t.Run("jsonparser read-path reads activation from the gateway payload shape", func(t *testing.T) {
		// This subtest reads the wire bytes directly with jsonparser (no reporter),
		// so WorkspaceId is intentionally omitted — only the payload/params matter.
		job := &jobsdb.JobT{
			Parameters: buildGatewayParams(t),
			EventPayload: buildGatewayPayload(t, map[string]any{
				"fingerprint": "fp-abc",
				"origin":      "data-graph-audience",
			}),
		}

		// Array index MUST use bracket notation "[0]" — a bare "0" is parsed as an
		// object key and returns "" (mirror records_reporter.go read path).
		fingerprint := jsonparser.GetStringOrEmpty(job.EventPayload, "batch", "[0]", "context", "activation", "fingerprint")
		origin := jsonparser.GetStringOrEmpty(job.EventPayload, "batch", "[0]", "context", "activation", "origin")

		require.Equal(t, "fp-abc", fingerprint)
		require.NotEmpty(t, fingerprint)
		require.Equal(t, "data-graph-audience", origin)
		require.NotEmpty(t, origin)

		// Parameters carry the connection grain (mirror gateway/handle.go:485,491-493).
		require.Equal(t, "src-1", jsonparser.GetStringOrEmpty(job.Parameters, "source_id"))
		require.Equal(t, "dst-1", jsonparser.GetStringOrEmpty(job.Parameters, "destination_id"))
	})

	// 2) HLL wire-compat — encode via the production path, then decode the raw
	// bytes the way the backend does and prove a clean round-trip.
	t.Run("HLL byte round-trip under Log2m=16 Regwidth=5", func(t *testing.T) {
		reporter := newWireCompatReporter(t)
		job := &jobsdb.JobT{
			WorkspaceId: "ws-1",
			Parameters:  buildGatewayParams(t),
			EventPayload: buildGatewayPayload(t, map[string]any{
				"fingerprint": "fp-abc",
				"origin":      "data-graph-audience",
			}),
		}

		reports := reporter.GenerateReportsFromJobs([]*jobsdb.JobT{job})
		require.Len(t, reports, 1)

		// Production encode path: ReportActivationRecords stores hllToString(hll),
		// which hex-encodes hll.ToBytes(). Round-trip exactly what lands in postgres.
		encoded, err := reporter.hllToString(reports[0].FingerprintHll)
		require.NoError(t, err)

		decoded, err := hex.DecodeString(encoded)
		require.NoError(t, err)

		// rudderstack-reporting's postgres `hll` extension decodes these bytes with
		// Log2m=16, Regwidth=5. Changing Log2m/Regwidth breaks cross-service
		// wire-compat: the backend would mis-decode the header or refuse to merge
		// the sketch — silent corruption, not a crash. Keep these two values
		// character-for-character identical to the reporter's hllSettings.
		roundTripped, err := hll.FromBytes(decoded)
		require.NoError(t, err)
		require.Equal(t, 16, roundTripped.Settings().Log2m)
		require.Equal(t, 5, roundTripped.Settings().Regwidth)
		require.Equal(t, uint64(1), roundTripped.Cardinality())
	})
}
