package activationrecords

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

func TestUniqueActivationRecordsReporter(t *testing.T) {
	// prepareJob builds a job with the standard activation payload shape.
	prepareJob := func(sourceID, destinationID, fingerprint, origin, workspaceID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			Parameters:   fmt.Appendf(nil, `{"source_id":%q,"destination_id":%q}`, sourceID, destinationID),
			EventPayload: fmt.Appendf(nil, `{"batch":[{"context":{"activation":{"fingerprint":%q,"origin":%q}}}]}`, fingerprint, origin),
			UserID:       uuid.NewString(),
			UUID:         uuid.New(),
			CustomVal:    "GW",
			WorkspaceId:  workspaceID,
		}
	}

	// prepareJobNoDstID builds a job whose Parameters JSON omits the destination_id key entirely.
	prepareJobNoDstID := func(sourceID, fingerprint, origin, workspaceID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			Parameters:   fmt.Appendf(nil, `{"source_id":%q}`, sourceID),
			EventPayload: fmt.Appendf(nil, `{"batch":[{"context":{"activation":{"fingerprint":%q,"origin":%q}}}]}`, fingerprint, origin),
			UserID:       uuid.NewString(),
			UUID:         uuid.New(),
			CustomVal:    "GW",
			WorkspaceId:  workspaceID,
		}
	}

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
				name: "both fields present - one report on right grain, origin carried",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "fp1", "org1", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Len(t, reports, 1)
					r := reports[0]
					require.Equal(t, "ws1", r.WorkspaceID)
					require.Equal(t, "src1", r.SourceID)
					require.Equal(t, "dst1", r.DestinationID)
					require.Equal(t, "org1", r.Origin)
					require.NotNil(t, r.FingerprintHll)
					require.Equal(t, uint64(1), r.FingerprintHll.Cardinality())
				},
			},
			{
				name: "missing fingerprint - skip",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "", "org1", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Empty(t, reports)
				},
			},
			{
				name: "missing origin - skip",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "fp1", "", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Empty(t, reports)
				},
			},
			{
				name: "missing destination_id in params - skip",
				jobs: []*jobsdb.JobT{
					prepareJobNoDstID("src1", "fp1", "org1", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Empty(t, reports)
				},
			},
			{
				name: "duplicate fingerprint same connection - cardinality 1",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "fp1", "org1", "ws1"),
					prepareJob("src1", "dst1", "fp1", "org1", "ws1"),
				},
				verify: func(t *testing.T, reports []*ActivationRecord) {
					require.Len(t, reports, 1)
					require.Equal(t, uint64(1), reports[0].FingerprintHll.Cardinality())
				},
			},
			{
				name: "two destinations same fingerprint - two reports",
				jobs: []*jobsdb.JobT{
					prepareJob("src1", "dst1", "fp1", "org1", "ws1"),
					prepareJob("src1", "dst2", "fp1", "org1", "ws1"),
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

	t.Run("HLLSettings", func(t *testing.T) {
		reporter, err := NewUniqueActivationRecordsReporter(logger.NOP, config.Default, stats.NOP)
		require.NoError(t, err)
		require.Equal(t, 16, reporter.hllSettings.Log2m)
		require.Equal(t, 5, reporter.hllSettings.Regwidth)
	})
}
