package activationrecords

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/go-hll"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

func TestUniqueActivationRecordsReporter(t *testing.T) {
	// prepareJob builds a job with the standard activation payload shape. rETL jobs
	// carry source_category=warehouse, the grain MAR meters.
	prepareJob := func(sourceID, destinationID, fingerprint, workspaceID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			Parameters:   fmt.Appendf(nil, `{"source_id":%q,"destination_id":%q,"source_category":"warehouse"}`, sourceID, destinationID),
			EventPayload: fmt.Appendf(nil, `{"batch":[{"context":{"activation":{"fingerprint":%q}}}]}`, fingerprint),
			UserID:       uuid.NewString(),
			UUID:         uuid.New(),
			CustomVal:    "GW",
			WorkspaceId:  workspaceID,
		}
	}

	// prepareJobNoDstID builds a job whose Parameters JSON omits the destination_id key entirely.
	prepareJobNoDstID := func(sourceID, fingerprint, workspaceID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			Parameters:   fmt.Appendf(nil, `{"source_id":%q,"source_category":"warehouse"}`, sourceID),
			EventPayload: fmt.Appendf(nil, `{"batch":[{"context":{"activation":{"fingerprint":%q}}}]}`, fingerprint),
			UserID:       uuid.NewString(),
			UUID:         uuid.New(),
			CustomVal:    "GW",
			WorkspaceId:  workspaceID,
		}
	}

	t.Run("constructor validates HLL settings", func(t *testing.T) {
		// Default settings are valid.
		_, err := NewUniqueActivationRecordsReporter(logger.NOP, config.Default, stats.NOP)
		require.NoError(t, err)

		// An out-of-range precision (Log2m valid range is [4, 31]) must fail fast at
		// construction rather than panicking later in the processor hot path.
		badConf := config.New()
		badConf.Set("ActivationRecords.precision", 99)
		_, err = NewUniqueActivationRecordsReporter(logger.NOP, badConf, stats.NOP)
		require.Error(t, err)
	})

	t.Run("GenerateReportsFromJobs", func(t *testing.T) {
		reporter, err := NewUniqueActivationRecordsReporter(logger.NOP, config.Default, stats.NOP)
		require.NoError(t, err)

		testCases := []struct {
			name string
			jobs []*jobsdb.JobT
			// verify is called with the resulting reports slice.
			verify func(t *testing.T, reports []*ActivationRecord)
		}{
			{
				name: "fingerprint present - one report on right grain",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "fp1", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Len(t, reports, 1)
					r := reports[0]
					require.Equal(t, "ws1", r.WorkspaceID)
					require.Equal(t, "src1", r.SourceID)
					require.Equal(t, "dst1", r.DestinationID)
					require.NotNil(t, r.FingerprintHll)
					require.Equal(t, uint64(1), r.FingerprintHll.Cardinality())
				},
			},
			{
				name: "missing fingerprint - skip",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Empty(t, reports)
				},
			},
			{
				name: "missing destination_id in params - skip",
				jobs: []*jobsdb.JobT{
					prepareJobNoDstID("src1", "fp1", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Empty(t, reports)
				},
			},
			{
				name: "non-rETL source category - skip even with fingerprint",
				jobs: []*jobsdb.JobT{
					{
						// An event-stream source whose payload carries a (client-stamped)
						// fingerprint must NOT be metered: MAR meters warehouse sources only.
						Parameters:   fmt.Appendf(nil, `{"source_id":%q,"destination_id":%q,"source_category":"eventStream"}`, "src1", "dst1"),
						EventPayload: []byte(`{"batch":[{"context":{"activation":{"fingerprint":"fp-injected"}}}]}`),
						UserID:       uuid.NewString(),
						UUID:         uuid.New(),
						CustomVal:    "GW",
						WorkspaceId:  "ws1",
					},
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Empty(t, reports)
				},
			},
			{
				name: "duplicate fingerprint same connection - cardinality 1",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "fp1", "ws1"),
					prepareJob("src1", "dst1", "fp1", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Len(t, reports, 1)
					require.Equal(t, uint64(1), reports[0].FingerprintHll.Cardinality())
				},
			},
			{
				name: "two destinations same fingerprint - two reports",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "fp1", "ws1"),
					prepareJob("src1", "dst2", "fp1", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Len(t, reports, 2)
					dstIDs := make([]string, 0, len(reports))
					for _, r := range reports {
						dstIDs = append(dstIDs, r.DestinationID)
						require.Equal(t, uint64(1), r.FingerprintHll.Cardinality())
					}
					require.ElementsMatch(t, []string{"dst1", "dst2"}, dstIDs)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				reports := reporter.GenerateReportsFromJobs(tc.jobs)
				tc.verify(t, reports)
			})
		}
	})

	t.Run("GenerateReportsFromJobs_MultiEventBatch", func(t *testing.T) {
		reporter, err := NewUniqueActivationRecordsReporter(logger.NOP, config.Default, stats.NOP)
		require.NoError(t, err)

		// prepareTwoEventJob builds a job whose batch has TWO events with distinct fingerprints.
		prepareTwoEventJob := func(sourceID, destinationID, fp1, fp2, workspaceID string) *jobsdb.JobT {
			return &jobsdb.JobT{
				Parameters: fmt.Appendf(nil, `{"source_id":%q,"destination_id":%q,"source_category":"warehouse"}`, sourceID, destinationID),
				EventPayload: fmt.Appendf(nil,
					`{"batch":[{"context":{"activation":{"fingerprint":%q}}},{"context":{"activation":{"fingerprint":%q}}}]}`,
					fp1, fp2,
				),
				UserID:      uuid.NewString(),
				UUID:        uuid.New(),
				CustomVal:   "GW",
				WorkspaceId: workspaceID,
			}
		}

		t.Run("two distinct fingerprints in same batch => cardinality 2", func(t *testing.T) {
			job := prepareTwoEventJob("src1", "dst1", "fp-1", "fp-2", "ws1")
			reports := reporter.GenerateReportsFromJobs([]*jobsdb.JobT{job})
			require.Len(t, reports, 1)
			require.NotNil(t, reports[0].FingerprintHll)
			require.Equal(t, uint64(2), reports[0].FingerprintHll.Cardinality())
		})

		t.Run("batch with one valid and one invalid element - valid element still metered", func(t *testing.T) {
			// batch[0] has no fingerprint; batch[1] is valid.
			job := &jobsdb.JobT{
				Parameters:   fmt.Appendf(nil, `{"source_id":%q,"destination_id":%q,"source_category":"warehouse"}`, "src1", "dst1"),
				EventPayload: []byte(`{"batch":[{"context":{"activation":{}}},{"context":{"activation":{"fingerprint":"fp-valid"}}}]}`),
				UserID:       uuid.NewString(),
				UUID:         uuid.New(),
				CustomVal:    "GW",
				WorkspaceId:  "ws1",
			}
			reports := reporter.GenerateReportsFromJobs([]*jobsdb.JobT{job})
			require.Len(t, reports, 1)
			require.Equal(t, uint64(1), reports[0].FingerprintHll.Cardinality())
		})
	})

	t.Run("HLLSettings", func(t *testing.T) {
		reporter, err := NewUniqueActivationRecordsReporter(logger.NOP, config.Default, stats.NOP)
		require.NoError(t, err)
		require.Equal(t, 16, reporter.hllSettings.Log2m)
		require.Equal(t, 5, reporter.hllSettings.Regwidth)
	})

	t.Run("ForceFullSyncReplay", func(t *testing.T) {
		// A force-full-sync replays the SAME jobs in a SEPARATE reporting cycle.
		// The backend merges sketches across cycles (postgres hll union), so a
		// replay of the same fingerprint MUST NOT inflate the cardinality —
		// otherwise every retried sync would double-count monthly active records.
		reporter, err := NewUniqueActivationRecordsReporter(logger.NOP, config.Default, stats.NOP)
		require.NoError(t, err)

		job := prepareJob("src1", "dst1", "fp1", "ws1")

		first := reporter.GenerateReportsFromJobs([]*jobsdb.JobT{job})
		require.Len(t, first, 1)
		require.Equal(t, uint64(1), first[0].FingerprintHll.Cardinality())

		second := reporter.GenerateReportsFromJobs([]*jobsdb.JobT{job})
		require.Len(t, second, 1)
		require.Equal(t, uint64(1), second[0].FingerprintHll.Cardinality())

		// Merge the two cycles the way the backend would: union both sketches into
		// a fresh HLL built with the reporter's settings. Same fingerprint twice
		// across cycles still collapses to cardinality 1.
		union, err := hll.NewHll(*reporter.hllSettings)
		require.NoError(t, err)
		union.Union(*first[0].FingerprintHll)
		union.Union(*second[0].FingerprintHll)
		require.Equal(t, uint64(1), union.Cardinality())
	})
}
