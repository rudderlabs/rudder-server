package trackedusers

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

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
	hllSettings        = hll.Settings{Log2m: 14, Regwidth: 5, ExplicitThreshold: hll.AutoExplicitThreshold, SparseEnabled: true}
	sampleWorkspaceID  = "workspaceID"
	sampleWorkspaceID2 = "workspaceID2"
	sampleSourceID     = "sourceID"
	sampleTestJob1     = &jobsdb.JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    "GW",
		WorkspaceId:  "workspaceID",
	}

	sampleTestJob2 = &jobsdb.JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"userId":"user_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    "GW",
		WorkspaceId:  "workspaceID",
	}

	sampleTestJob3 = &jobsdb.JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"userId":"user_id","anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    "GW",
		WorkspaceId:  "workspaceID",
	}

	sampleTestJob4 = &jobsdb.JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"userId":"user_id_1","anonymousId_1":"anon_id_1","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    "GW",
		WorkspaceId:  "workspaceID",
	}

	prepareJob = func(sourceID, userID, annID, workspaceID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			Parameters:   []byte(fmt.Sprintf(`{"batch_id":1,"source_id":%q,"source_job_run_id":""}`, sourceID)),
			EventPayload: []byte(fmt.Sprintf(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]", "batch": [{"userId":%q,"anonymousId_1":%q,"channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`, userID, annID)),
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
			annIDHll.AddRaw(murmur3.Sum64WithSeed([]byte(uuid.NewString()), murmurSeed))
		}
		for i := 0; i < noOfIdentifiedAnnID; i++ {
			identifiedAnnIDHll.AddRaw(murmur3.Sum64WithSeed([]byte(uuid.NewString()), murmurSeed))
		}
		return &UsersReport{
			WorkspaceID:              workspaceID,
			SourceID:                 sourceID,
			UserIDHll:                &userIDHll,
			AnonymousIDHLL:           &annIDHll,
			IdentifiedAnonymousIDHLL: &identifiedAnnIDHll,
		}
	}
)

func TestUniqueUsersReporter(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Run("GenerateReportsFromJobs", func(t *testing.T) {
		testCases := []struct {
			name         string
			jobs         []*jobsdb.JobT
			trackedUsers map[string]map[string]int
		}{
			{
				name: "happy case",
				jobs: []*jobsdb.JobT{
					sampleTestJob1,
					sampleTestJob2,
					sampleTestJob3,
					sampleTestJob4,
				},
				trackedUsers: map[string]map[string]int{
					"workspaceID": {
						"sourceID": 2,
					},
				},
			},
			{
				name: "happy case 2",
				jobs: []*jobsdb.JobT{
					sampleTestJob1,
					sampleTestJob2,
					sampleTestJob3,
					sampleTestJob4,
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID),
					prepareJob(sampleSourceID, "user", "ann", sampleWorkspaceID2),
				},
				trackedUsers: map[string]map[string]int{
					sampleWorkspaceID: {
						sampleSourceID: 3,
					},
					sampleWorkspaceID2: {
						sampleSourceID: 1,
					},
				},
			},
			{
				name: "no source id in job parameters",
				jobs: []*jobsdb.JobT{
					prepareJob("", "user", "ann", sampleWorkspaceID),
					prepareJob("", "user", "ann", sampleWorkspaceID),
				},
				trackedUsers: map[string]map[string]int{},
			},
			{
				name: "no event tracked in reports",
				jobs: []*jobsdb.JobT{
					prepareJob("source", "", "", sampleWorkspaceID),
					prepareJob("", "user", "ann", sampleWorkspaceID),
					prepareJob("", "user", "ann", sampleWorkspaceID),
				},
				trackedUsers: map[string]map[string]int{},
			},
			{
				name: "no workspace id in job",
				jobs: []*jobsdb.JobT{
					prepareJob("source", "user", "ann", ""),
					prepareJob("source", "user", "ann", ""),
				},
				trackedUsers: map[string]map[string]int{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				collector := &UniqueUsersReporter{log: logger.NOP, hllSettings: &hllSettings}
				reports := collector.GenerateReportsFromJobs(tc.jobs)

				result := make(map[string]map[string]int)
				for _, e := range reports {
					if result[e.WorkspaceID] == nil {
						result[e.WorkspaceID] = make(map[string]int)
					}
					if e.UserIDHll != nil {
						result[e.WorkspaceID][e.SourceID] += int(e.UserIDHll.Cardinality())
					}
					if e.AnonymousIDHLL != nil {
						result[e.WorkspaceID][e.SourceID] += int(e.AnonymousIDHLL.Cardinality())
					}
					if e.IdentifiedAnonymousIDHLL != nil {
						result[e.WorkspaceID][e.SourceID] -= int(e.IdentifiedAnonymousIDHLL.Cardinality())
					}
				}
				require.Equal(t, tc.trackedUsers, result)
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
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				postgresContainer, err := postgres.Setup(pool, t)
				require.NoError(t, err)

				collector, err := NewUniqueUsersReporter(logger.NOP, nil)
				require.NoError(t, err)

				sqlTx, err := postgresContainer.DB.Begin()
				require.NoError(t, err)
				tx := &txn.Tx{Tx: sqlTx}

				err = collector.ReportUsers(context.Background(), tc.reports, tx)
				if tc.shouldFail {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				require.NoError(t, tx.Commit())

				rows, err := postgresContainer.DB.Query("SELECT workspace_id, source_id, userid_hll, anonymousid_hll, identified_anonymousid_hll FROM tracked_users_reports")
				require.NoError(t, err)
				require.NoError(t, rows.Err())
				defer func() { _ = rows.Close() }()
				var entry UsersReport
				entries := make([]UsersReport, 0)
				for rows.Next() {
					var userIDHllStr, annIDHllStr, combHllStr string
					err = rows.Scan(&entry.WorkspaceID, &entry.SourceID, &userIDHllStr, &annIDHllStr, &combHllStr)
					require.NoError(t, err)
					userHllBytes, err := hex.DecodeString(userIDHllStr[2:])
					require.NoError(t, err)
					userHll, err := hll.FromBytes(userHllBytes)
					require.NoError(t, err)
					entry.UserIDHll = &userHll
					annIDHllBytes, err := hex.DecodeString(annIDHllStr[2:])
					require.NoError(t, err)
					annHll, err := hll.FromBytes(annIDHllBytes)
					require.NoError(t, err)
					entry.AnonymousIDHLL = &annHll
					combineHllBytes, err := hex.DecodeString(combHllStr[2:])
					require.NoError(t, err)
					combHll, err := hll.FromBytes(combineHllBytes)
					require.NoError(t, err)
					entry.IdentifiedAnonymousIDHLL = &combHll
					entries = append(entries, entry)
				}
				result := make(map[string]map[string]int)
				for _, e := range entries {
					if result[e.WorkspaceID] == nil {
						result[e.WorkspaceID] = make(map[string]int)
					}
					result[e.WorkspaceID][e.SourceID] += int(e.UserIDHll.Cardinality())
					result[e.WorkspaceID][e.SourceID] += int(e.AnonymousIDHLL.Cardinality())
					result[e.WorkspaceID][e.SourceID] -= int(e.IdentifiedAnonymousIDHLL.Cardinality())
				}
				require.Equal(t, tc.trackedUsers, result)
			})
		}
	})
}
