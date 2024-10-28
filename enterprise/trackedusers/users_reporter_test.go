package trackedusers

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/spaolacci/murmur3"

	"github.com/segmentio/go-hll"

	txn "github.com/rudderlabs/rudder-server/utils/tx"

	"github.com/google/uuid"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

var (
	hllSettings          = hll.Settings{Log2m: 16, Regwidth: 5, ExplicitThreshold: hll.AutoExplicitThreshold, SparseEnabled: true}
	sampleWorkspaceID    = "workspaceID"
	sampleWorkspaceID2   = "workspaceID2"
	sampleSourceID       = "sourceID"
	sampleSourceToFilter = "filtered-source-id"

	prepareJob = func(sourceID, userID, annID, workspaceID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			Parameters:   []byte(fmt.Sprintf(`{"source_id":%q}`, sourceID)),
			EventPayload: []byte(fmt.Sprintf(`{"batch": [{"anonymousId":%q,"userId":%q,"type":"track"}]}`, annID, userID)),
			UserID:       uuid.NewString(),
			UUID:         uuid.New(),
			CustomVal:    "GW",
			WorkspaceId:  workspaceID,
		}
	}
	prepareAliasJob = func(sourceID, userID, previousID, workspaceID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			Parameters:   []byte(fmt.Sprintf(`{"source_id":%q}`, sourceID)),
			EventPayload: []byte(fmt.Sprintf(`{"batch": [{"previousId":%q,"userId":%q,"type":"alias"}]}`, previousID, userID)),
			UserID:       uuid.NewString(),
			UUID:         uuid.New(),
			CustomVal:    "GW",
			WorkspaceId:  workspaceID,
		}
	}
	prepareUserReport = func(t *testing.T, sourceID, workspaceID string, noOfUserIDs, noOfAnnID, noOfIdentifiedAnnID int) *UsersReport {
		userIDHll, _ := hll.NewHll(hllSettings)
		annIDHll, _ := hll.NewHll(hllSettings)
		identifiedAnnIDHll, _ := hll.NewHll(hllSettings)
		for i := 0; i < noOfUserIDs; i++ {
			userIDHll.AddRaw(murmur3.Sum64WithSeed([]byte(uuid.NewString()), murmurSeed))
		}
		for i := 0; i < noOfAnnID; i++ {
			userIDHll.AddRaw(murmur3.Sum64WithSeed([]byte(uuid.NewString()), murmurSeed))
		}
		for i := 0; i < noOfIdentifiedAnnID; i++ {
			identifiedAnnIDHll.AddRaw(murmur3.Sum64WithSeed([]byte(uuid.NewString()), murmurSeed))
		}
		report := &UsersReport{
			WorkspaceID:              workspaceID,
			SourceID:                 sourceID,
			UserIDHll:                &userIDHll,
			AnonymousIDHll:           &annIDHll,
			IdentifiedAnonymousIDHll: &identifiedAnnIDHll,
		}

		if noOfUserIDs == 0 {
			report.UserIDHll = nil
		}
		if noOfAnnID == 0 {
			report.AnonymousIDHll = nil
		}
		if noOfIdentifiedAnnID == 0 {
			report.IdentifiedAnonymousIDHll = nil
		}
		return report
	}
)

func TestUniqueUsersReporter(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Run("GenerateReportsFromJobs", func(t *testing.T) {
		testCases := []struct {
			name             string
			jobs             []*jobsdb.JobT
			sourceIDtoFilter map[string]bool
			trackedUsers     []*UsersReport
		}{
			{
				name: "happy case",
				jobs: []*jobsdb.JobT{
					prepareJob(sampleSourceID, "", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id_1", "anon_id_1", sampleWorkspaceID),
				},
				trackedUsers: []*UsersReport{
					{
						WorkspaceID: sampleWorkspaceID,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id_1"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id_1"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id", "anon_id")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id_1", "anon_id_1")), murmurSeed))
							return &resHll
						}(),
					},
				},
			},
			{
				name: "happy case 2",
				jobs: []*jobsdb.JobT{
					prepareJob(sampleSourceID, "", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id_1", "anon_id_1", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID2),
				},
				trackedUsers: []*UsersReport{
					{
						WorkspaceID: sampleWorkspaceID,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id_1"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id_1"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id", "anon_id")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id_1", "anon_id_1")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user", "ann")), murmurSeed))
							return &resHll
						}(),
					},
					{
						WorkspaceID: sampleWorkspaceID2,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user", "ann")), murmurSeed))
							return &resHll
						}(),
					},
				},
			},
			{
				name: "happy case - alias jobs",
				jobs: []*jobsdb.JobT{
					prepareJob(sampleSourceID, "", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id_1", "anon_id_1", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID2),
					prepareAliasJob(sampleSourceID, "user_id", "user_id_1", sampleWorkspaceID),
				},
				trackedUsers: []*UsersReport{
					{
						WorkspaceID: sampleWorkspaceID,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id_1"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id_1"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id", "anon_id")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id_1", "anon_id_1")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user", "ann")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id_1"), murmurSeed))
							return &resHll
						}(),
					},
					{
						WorkspaceID: sampleWorkspaceID2,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user", "ann")), murmurSeed))
							return &resHll
						}(),
					},
				},
			},
			{
				name: "alias jobs with prevID same as userID",
				jobs: []*jobsdb.JobT{
					prepareJob(sampleSourceID, "", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id_1", "anon_id_1", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID2),
					prepareAliasJob(sampleSourceID, "user_id", "user_id", sampleWorkspaceID),
				},
				trackedUsers: []*UsersReport{
					{
						WorkspaceID: sampleWorkspaceID,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id_1"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id_1"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id", "anon_id")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id_1", "anon_id_1")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user", "ann")), murmurSeed))
							return &resHll
						}(),
					},
					{
						WorkspaceID: sampleWorkspaceID2,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user", "ann")), murmurSeed))
							return &resHll
						}(),
					},
				},
			},
			{
				name: "happy case - no identified use ids",
				jobs: []*jobsdb.JobT{
					prepareJob(sampleSourceID, "", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "", sampleWorkspaceID),
					prepareJob(sampleSourceID, "", "ann", sampleWorkspaceID2),
				},
				trackedUsers: []*UsersReport{
					{
						WorkspaceID: sampleWorkspaceID,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll:           nil,
						IdentifiedAnonymousIDHll: nil,
					},
					{
						WorkspaceID: sampleWorkspaceID2,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll:           nil,
						IdentifiedAnonymousIDHll: nil,
					},
				},
			},
			{
				name: "happy case - same user and anonymous id",
				jobs: []*jobsdb.JobT{
					prepareJob(sampleSourceID, "anon_id", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "user_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "", sampleWorkspaceID),
					prepareJob(sampleSourceID, "ann", "ann", sampleWorkspaceID2),
				},
				trackedUsers: []*UsersReport{
					{
						WorkspaceID: sampleWorkspaceID,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll:           nil,
						IdentifiedAnonymousIDHll: nil,
					},
					{
						WorkspaceID: sampleWorkspaceID2,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll:           nil,
						IdentifiedAnonymousIDHll: nil,
					},
				},
			},
			{
				name: "filter non event stream sources",
				jobs: []*jobsdb.JobT{
					prepareJob(sampleSourceID, "", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id", "anon_id", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user_id_1", "anon_id_1", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID2),
					prepareJob(sampleSourceToFilter, uuid.NewString(), uuid.NewString(), sampleWorkspaceID2),
				},
				sourceIDtoFilter: map[string]bool{
					sampleSourceToFilter: true,
				},
				trackedUsers: []*UsersReport{
					{
						WorkspaceID: sampleWorkspaceID,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user_id_1"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("anon_id_1"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id", "anon_id")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user_id_1", "anon_id_1")), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user", "ann")), murmurSeed))
							return &resHll
						}(),
					},
					{
						WorkspaceID: sampleWorkspaceID2,
						SourceID:    sampleSourceID,
						UserIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("user"), murmurSeed))
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte("ann"), murmurSeed))
							return &resHll
						}(),
						AnonymousIDHll: nil,
						IdentifiedAnonymousIDHll: func() *hll.Hll {
							resHll, err := hll.NewHll(hllSettings)
							require.NoError(t, err)
							resHll.AddRaw(murmur3.Sum64WithSeed([]byte(
								combineUserIDAnonymousID("user", "ann")), murmurSeed))
							return &resHll
						}(),
					},
				},
			},
			{
				name: "no source id in job parameters",
				jobs: []*jobsdb.JobT{
					prepareJob("", "user", "ann", sampleWorkspaceID),
					prepareJob("", "user", "ann", sampleWorkspaceID),
				},
				trackedUsers: nil,
			},
			{
				name: "no event tracked in reports",
				jobs: []*jobsdb.JobT{
					prepareJob("source", "", "", sampleWorkspaceID),
					prepareJob("", "user", "ann", sampleWorkspaceID),
					prepareJob("", "user", "ann", sampleWorkspaceID),
				},
				trackedUsers: nil,
			},
			{
				name: "no workspace id in job",
				jobs: []*jobsdb.JobT{
					prepareJob("source", "user", "ann", ""),
					prepareJob("source", "user", "ann", ""),
				},
				trackedUsers: nil,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				collector := &UniqueUsersReporter{log: logger.NOP, hllSettings: &hllSettings}
				reports := collector.GenerateReportsFromJobs(tc.jobs, tc.sourceIDtoFilter)
				require.ElementsMatch(t, tc.trackedUsers, reports)
			})
		}
	})

	t.Run("ReportUsers", func(t *testing.T) {
		testCases := []struct {
			name         string
			reports      []*UsersReport
			trackedUsers map[string]map[string]int
			shouldFail   bool
		}{
			{
				name: "happy case",
				reports: []*UsersReport{
					prepareUserReport(t, sampleSourceID, sampleWorkspaceID, 3, 5, 3),
				},
				trackedUsers: map[string]map[string]int{
					sampleWorkspaceID: {
						sampleSourceID: 5,
					},
				},
			},
			{
				name: "happy case - no identified anon id",
				reports: []*UsersReport{
					prepareUserReport(t, sampleSourceID, sampleWorkspaceID, 3, 3, 0),
				},
				trackedUsers: map[string]map[string]int{
					sampleWorkspaceID: {
						sampleSourceID: 6,
					},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				postgresContainer, err := postgres.Setup(pool, t)
				require.NoError(t, err)

				collector, err := NewUniqueUsersReporter(logger.NOP, config.Default, stats.NOP)
				require.NoError(t, err)
				err = collector.MigrateDatabase(postgresContainer.DBDsn, config.Default)
				require.NoError(t, err)
				sqlTx, err := postgresContainer.DB.Begin()
				require.NoError(t, err)
				tx := &txn.Tx{Tx: sqlTx}
				fixedTime := time.Date(2021, 6, 6, 20, 26, 39, 598000000, time.UTC)
				collector.now = func() time.Time {
					return fixedTime
				}

				err = collector.ReportUsers(context.Background(), tc.reports, tx)
				if tc.shouldFail {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				require.NoError(t, tx.Commit())

				rows, err := postgresContainer.DB.Query("SELECT workspace_id, source_id, userid_hll, anonymousid_hll, identified_anonymousid_hll, reported_at FROM tracked_users_reports")
				require.NoError(t, err)
				require.NoError(t, rows.Err())
				defer func() { _ = rows.Close() }()
				var entry UsersReport
				entries := make([]UsersReport, 0)
				for rows.Next() {
					var userIDHllStr, annIDHllStr, combHllStr string
					var reportedTime time.Time
					err = rows.Scan(&entry.WorkspaceID, &entry.SourceID, &userIDHllStr, &annIDHllStr, &combHllStr, &reportedTime)
					require.NoError(t, err)
					userHllBytes, err := hex.DecodeString(userIDHllStr)
					require.NoError(t, err)
					userHll, err := hll.FromBytes(userHllBytes)
					require.NoError(t, err)
					entry.UserIDHll = &userHll
					annIDHllBytes, err := hex.DecodeString(annIDHllStr)
					require.NoError(t, err)
					annHll, err := hll.FromBytes(annIDHllBytes)
					require.NoError(t, err)
					entry.AnonymousIDHll = &annHll
					combineHllBytes, err := hex.DecodeString(combHllStr)
					require.NoError(t, err)
					combHll, err := hll.FromBytes(combineHllBytes)
					require.NoError(t, err)
					entry.IdentifiedAnonymousIDHll = &combHll
					entries = append(entries, entry)
					require.Equal(t, fixedTime, reportedTime)
				}
				result := make(map[string]map[string]int)
				for _, e := range entries {
					if result[e.WorkspaceID] == nil {
						result[e.WorkspaceID] = make(map[string]int)
					}
					result[e.WorkspaceID][e.SourceID] += int(e.UserIDHll.Cardinality())
					result[e.WorkspaceID][e.SourceID] += int(e.AnonymousIDHll.Cardinality())
					result[e.WorkspaceID][e.SourceID] -= int(e.IdentifiedAnonymousIDHll.Cardinality())
				}
				require.Equal(t, tc.trackedUsers, result)
			})
		}
	})
}
