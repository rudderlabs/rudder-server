package snowpipestreaming

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

// TestManagerV2Integration runs the high-performance REST path end to end against a real Snowflake account.
// It needs the same credentials as the classic integration tests, and no snowpipe-clients.
func TestManagerV2Integration(t *testing.T) {
	for _, key := range []string{testhelper.TestKeyPairUnencrypted, testhelper.TestKeyPairEncrypted} {
		if _, exists := os.LookupEnv(key); !exists {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatalf("%s environment variable not set", key)
			}
			t.Skipf("Skipping %s as %s is not set", t.Name(), key)
		}
	}
	validations.Init()

	t.Run("unencrypted key", func(t *testing.T) { runHPIntegration(t, testhelper.TestKeyPairUnencrypted, true) })
	t.Run("encrypted key", func(t *testing.T) { runHPIntegration(t, testhelper.TestKeyPairEncrypted, false) })
}

type hpLive struct {
	t           *testing.T
	clockOffset atomic.Int64 // added to the managers' clock, e.g. to expire cached schemas
	ctx         context.Context
	conf        *config.Config
	destination *backendconfig.DestinationT
	namespace   string
	db          *sql.DB
}

func runHPIntegration(t *testing.T, credsKey string, full bool) {
	creds, err := testhelper.GetSnowpipeTestCredentials(credsKey)
	require.NoError(t, err)
	ctx := context.Background()
	namespace := testhelper.RandSchema()

	destination := backendconfigtest.NewDestinationBuilder("SNOWPIPE_STREAMING").
		WithID("hp-"+hex.EncodeToString(randomBytes(4))).
		WithConfigOption("account", creds.Account).
		WithConfigOption("warehouse", creds.Warehouse).
		WithConfigOption("database", creds.Database).
		WithConfigOption("role", creds.Role).
		WithConfigOption("user", creds.User).
		WithConfigOption("useKeyPairAuth", true).
		WithConfigOption("privateKey", creds.PrivateKey).
		WithConfigOption("privateKeyPassphrase", creds.PrivateKeyPassphrase).
		WithConfigOption("namespace", namespace).
		Build()

	// A SQL connection for assertions, and to drop the schema afterwards.
	sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
	require.NoError(t, sm.Setup(ctx, whutils.ModelWarehouse{Namespace: namespace, Destination: destination}, whutils.NewNoOpUploader()))
	t.Cleanup(func() { sm.Cleanup(ctx) })
	t.Cleanup(func() {
		_, err := sm.DB.DB.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, namespace))
		require.NoError(t, err)
	})

	conf := config.New()
	conf.Set("INSTANCE_ID", "hp-it-0")
	conf.Set("SnowpipeStreaming.maxBufferCapacity", 64*1024*1024)
	conf.Set("SnowpipeStreaming.maxInsertRequestSizeBytes", 64*1024*1024)
	// Short, so that imports Snowflake never commits (e.g. right after a table is recreated) are recovered within a test.
	conf.Set("SnowpipeStreaming.stuckPipelineThresholdInMinutes", 1)
	live := &hpLive{t: t, ctx: ctx, conf: conf, destination: &destination, namespace: namespace, db: sm.DB.DB}

	t.Run("dispatcher sends the batch on the high-performance path: schema and tables created, rows committed, JSON native", func(t *testing.T) {
		conf.Set("SnowpipeStreaming.highPerformance.enabled", true)
		d := NewManager(conf, logger.NOP, stats.NOP, live.destination)

		file, jobIDs := writeBatch(t, liveTrack(1, "a"), liveTrack(2, "b"), livePage(3), liveTrack(4, "c"))
		out := d.Upload(ctx, live.asyncDest(file, jobIDs))
		require.Empty(t, out.FailedJobIDs, out.FailedReason)
		require.Empty(t, out.AbortJobIDs, out.AbortReason)
		require.ElementsMatch(t, jobIDs, out.ImportingJobIDs)

		importID, infos := importIDOf(t, out)
		for _, info := range infos {
			require.Equal(t, versionHP, info.Version)
		}
		live.pollUntilDone(d, importID, false)

		require.Equal(t, 3, live.count("TRACKS"))
		require.Equal(t, 1, live.count("PAGES"))
		require.Equal(t, "OBJECT", live.scalar(`SELECT TYPEOF(CONTEXT) FROM %s WHERE ID = 'id-1'`, "TRACKS"), "VARIANT values are stored as objects, not text")
		require.Equal(t, `["x","y"]`, live.scalar(`SELECT TAGS FROM %s WHERE ID = 'id-1'`, "TRACKS"))
		require.Equal(t, "a", live.scalar(`SELECT CONTEXT:name::string FROM %s WHERE ID = 'id-1'`, "TRACKS"))
		require.Equal(t, "1795514400123", live.scalar(`SELECT DATE_PART(EPOCH_MILLISECOND, RECEIVED_AT) FROM %s WHERE ID = 'id-1'`, "TRACKS"), "2026-11-24T10:00:00.123Z, in UTC")
	})

	if !full {
		return // for the encrypted key, proving that it authenticates and loads is enough
	}

	statsStore, err := memstats.New()
	require.NoError(t, err)
	m := live.newManager(statsStore)

	t.Run("a new column is added before sending and lands on the open channel", func(t *testing.T) {
		e := liveTrack(10, "d")
		e.columns["PLAN"] = "string"
		e.data["PLAN"] = "pro"
		file, jobIDs := writeBatch(t, e)
		out := m.Upload(ctx, live.asyncDest(file, jobIDs))
		require.Equal(t, jobIDs, out.ImportingJobIDs, out.FailedReason+out.AbortReason)
		importID, _ := importIDOf(t, out)
		live.pollUntilDone(m, importID, false)
		require.Equal(t, "pro", live.scalar(`SELECT PLAN FROM %s WHERE ID = 'id-10'`, "TRACKS"))
	})

	t.Run("a value of the wrong type goes to the discards table", func(t *testing.T) {
		e := liveTrack(11, "e")
		e.columns["AMOUNT"] = "int"
		e.data["AMOUNT"] = 5
		file, jobIDs := writeBatch(t, e)
		importID, _ := importIDOf(t, m.Upload(ctx, live.asyncDest(file, jobIDs)))
		live.pollUntilDone(m, importID, false)

		bad := liveTrack(12, "f")
		bad.columns["AMOUNT"] = "string"
		bad.data["AMOUNT"] = "lots"
		file, jobIDs = writeBatch(t, bad)
		out := m.Upload(ctx, live.asyncDest(file, jobIDs))
		require.Equal(t, jobIDs, out.ImportingJobIDs, out.FailedReason+out.AbortReason)
		importID, infos := importIDOf(t, out)
		require.Len(t, infos, 2)
		live.pollUntilDone(m, importID, false)

		require.Equal(t, "lots", live.scalar(`SELECT COLUMN_VALUE FROM %s WHERE ROW_ID = 'id-12' AND COLUMN_NAME = 'AMOUNT'`, "RUDDER_DISCARDS"))
		require.Equal(t, "", live.scalar(`SELECT COALESCE(TO_VARCHAR(AMOUNT), '') FROM %s WHERE ID = 'id-12'`, "TRACKS"))
	})

	t.Run("a batch larger than a request is split in ordered chunks, and a row too large for any request is aborted alone", func(t *testing.T) {
		var events []testEvent
		for i := int64(20); i < 24; i++ {
			e := liveTrack(i, "big")
			e.data["EVENT"] = hex.EncodeToString(randomBytes(1200 * 1024)) // 2.4 MB, compresses to ~1.3 MB
			events = append(events, e)
		}
		huge := liveTrack(24, "huge")
		huge.data["EVENT"] = hex.EncodeToString(randomBytes(5 * 1024 * 1024)) // ~5.4 MB compressed: no request can hold it
		events = append(events, huge, liveTrack(25, "after"))

		file, jobIDs := writeBatch(t, events...)
		out := m.Upload(ctx, live.asyncDest(file, jobIDs))
		require.Equal(t, []int64{24}, out.AbortJobIDs)
		require.Equal(t, []int64{20, 21, 22, 23, 25}, out.ImportingJobIDs, out.FailedReason)
		importID, infos := importIDOf(t, out)
		require.Equal(t, "25", infos[0].Offset)
		live.pollUntilDone(m, importID, false)
		require.Equal(t, 5, live.int(`SELECT COUNT(*) FROM %s WHERE ID IN ('id-20','id-21','id-22','id-23','id-25')`, "TRACKS"))
	})

	t.Run("rows Snowflake rejects are counted, and don't fail the import", func(t *testing.T) {
		e := liveTrack(30, "g")
		e.columns["AMOUNT"] = "int"
		e.data["AMOUNT"] = "not-a-number" // same declared type as the column, so no discard: Snowflake skips the row
		file, jobIDs := writeBatch(t, e, liveTrack(31, "h"))
		out := m.Upload(ctx, live.asyncDest(file, jobIDs))
		importID, _ := importIDOf(t, out)
		res := live.pollUntilDone(m, importID, false)
		require.False(t, res.HasFailed)
		require.Equal(t, 0, live.int(`SELECT COUNT(*) FROM %s WHERE ID = 'id-30'`, "TRACKS"))
		require.Equal(t, 1, live.int(`SELECT COUNT(*) FROM %s WHERE ID = 'id-31'`, "TRACKS"))
		require.Eventually(t, func() bool {
			return statsStore.Get("snowpipe_streaming_row_errors", live.hpTags()).LastValue() >= 1
		}, time.Minute, time.Second)
	})

	t.Run("after a restart mid-import, the retry neither loses nor duplicates rows", func(t *testing.T) {
		events := []testEvent{liveTrack(40, "i"), liveTrack(41, "j"), liveTrack(42, "k")}
		file, jobIDs := writeBatch(t, events...)
		out := m.Upload(ctx, live.asyncDest(file, jobIDs))
		importID, _ := importIDOf(t, out)

		// A new process polls at once, without the channel's state: anything not committed yet is failed, and the
		// channel reopened so it can't commit later.
		restarted := live.newManager(stats.NOP, withState(newHPStates().get(live.destination.ID))) // a new process: no channel state
		res := live.pollUntilDone(restarted, importID, true)
		var retry []int64
		if res.HasFailed {
			st := uploadStats(t, restarted, file, jobIDs, res.FailedJobParameters)
			retry = st.Metadata.FailedKeys
			t.Logf("failed after restart: %v, succeeded: %v", st.Metadata.FailedKeys, st.Metadata.SucceededKeys)
		} else {
			t.Log("committed before the restarted poll")
		}
		if len(retry) > 0 {
			var retryEvents []testEvent
			for _, e := range events {
				for _, id := range retry {
					if e.jobID == id {
						retryEvents = append(retryEvents, e)
					}
				}
			}
			file, jobIDs := writeBatch(t, retryEvents...)
			out := restarted.Upload(ctx, live.asyncDest(file, jobIDs))
			importID, _ := importIDOf(t, out)
			live.pollUntilDone(restarted, importID, false)
		}
		countRows := func() int {
			return live.int(`SELECT COUNT(*) FROM %s WHERE ID IN ('id-40','id-41','id-42')`, "TRACKS")
		}
		require.Equal(t, 3, countRows())
		require.Equal(t, 3, live.int(`SELECT COUNT(DISTINCT ID) FROM %s WHERE ID IN ('id-40','id-41','id-42')`, "TRACKS"))
		// Nothing wrongly left in flight may commit later and duplicate a row.
		require.Never(t, func() bool { return countRows() != 3 }, 20*time.Second, 2*time.Second)
		m = restarted
	})

	// Each case warms the cache and opens PAGES' channel, changes the table behind its back, then sends batches until
	// one commits. Every job reported as committed must be in the table exactly once.
	for i, tc := range []struct {
		name string
		ddl  string
	}{
		{name: "a table dropped while its channel is open is recreated", ddl: `DROP TABLE %s`},
		{name: "a table replaced while its channel is open keeps loading", ddl: `CREATE OR REPLACE TABLE %s (ID VARCHAR, URL VARCHAR, RECEIVED_AT TIMESTAMP_TZ)`},
		{name: "a column dropped while its channel is open is added back once the schema cache expires", ddl: `ALTER TABLE %s DROP COLUMN URL`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := int64(100 * (i + 1))
			require.NotEmpty(t, live.sendUntilCommitted(m, base, base+10), "warm-up") // cache warm, channel open

			_, err := live.db.ExecContext(ctx, fmt.Sprintf(tc.ddl, fmt.Sprintf("%q.%q", namespace, "PAGES")))
			require.NoError(t, err)
			if strings.Contains(tc.ddl, "DROP COLUMN") {
				live.clockOffset.Store(int64(time.Hour)) // the schema cache expires
				defer live.clockOffset.Store(0)
			}

			committed := live.sendUntilCommitted(m, base+20, base+30)
			require.NotEmpty(t, committed, "loading resumes within a few batches")
			for _, id := range committed {
				require.Equal(t, 1, live.int(fmt.Sprintf(`SELECT COUNT(*) FROM %%s WHERE ID = 'page-%d'`, id), "PAGES"))
			}
			require.Equal(t, "https://example.com", live.scalar(fmt.Sprintf(`SELECT URL FROM %%s WHERE ID = 'page-%d'`, committed[0]), "PAGES"))
		})
	}
}

// sendUntilCommitted sends one page per batch, from job ID first, until a batch commits or job ID last is reached.
// It returns the job IDs of the batch that committed.
func (l *hpLive) sendUntilCommitted(m *ManagerV2, first, last int64) []int64 {
	l.t.Helper()
	for jobID := first; jobID <= last; jobID++ {
		file, jobIDs := writeBatch(l.t, livePage(jobID))
		out := m.Upload(l.ctx, l.asyncDest(file, jobIDs))
		l.t.Logf("job %d: importing=%v failed=%v aborted=%v %s%s", jobID, out.ImportingJobIDs, out.FailedJobIDs, out.AbortJobIDs, out.FailedReason, out.AbortReason)
		if len(out.ImportingJobIDs) == 0 {
			continue
		}
		importID, _ := importIDOf(l.t, out)
		res := l.pollUntilDone(m, importID, true)
		l.t.Logf("job %d: poll failed=%v %s", jobID, res.HasFailed, res.FailedJobParameters)
		if !res.HasFailed {
			return out.ImportingJobIDs
		}
	}
	return nil
}

func (l *hpLive) newManager(statsFactory stats.Stats, opts ...v2Opt) *ManagerV2 {
	clock := withClock(func() time.Time { return time.Now().Add(time.Duration(l.clockOffset.Load())) })
	return NewV2(l.conf, statsFactory, l.destination, New(l.conf, logger.NOP, statsFactory, l.destination), append([]v2Opt{clock}, opts...)...)
}

func (l *hpLive) hpTags() stats.Tags {
	return stats.Tags{"module": "batch_router", "workspaceId": l.destination.WorkspaceID, "destType": "SNOWPIPE_STREAMING", "destinationId": l.destination.ID, "version": "2"}
}

func (l *hpLive) asyncDest(file string, jobIDs []int64) *common.AsyncDestinationStruct {
	return &common.AsyncDestinationStruct{ImportingJobIDs: jobIDs, FileName: file, Count: len(jobIDs), Destination: l.destination}
}

// pollUntilDone polls until the imports leave the in-progress state. Unless allowFailure, they must succeed.
func (l *hpLive) pollUntilDone(m asyncManager, importID string, allowFailure bool) common.PollStatusResponse {
	l.t.Helper()
	var res common.PollStatusResponse
	require.Eventually(l.t, func() bool {
		res = m.Poll(l.ctx, common.AsyncPoll{ImportId: importID})
		return !res.InProgress
	}, 3*time.Minute, 2*time.Second, "import still in progress after 3 minutes")
	if !allowFailure {
		require.False(l.t, res.HasFailed, "import failed: %s %s", res.Error, res.FailedJobParameters)
	}
	return res
}

func (l *hpLive) count(table string) int {
	return l.int(`SELECT COUNT(*) FROM %s`, table)
}

func (l *hpLive) int(query, table string) int {
	l.t.Helper()
	n, err := strconv.Atoi(l.scalar(query, table))
	require.NoError(l.t, err)
	return n
}

func (l *hpLive) scalar(query, table string) string {
	l.t.Helper()
	var v sql.NullString
	q := fmt.Sprintf(query, fmt.Sprintf("%q.%q", l.namespace, table))
	require.NoError(l.t, l.db.QueryRowContext(l.ctx, q).Scan(&v), q)
	return v.String
}

func liveTrack(jobID int64, name string) testEvent {
	id := "id-" + strconv.FormatInt(jobID, 10)
	return testEvent{
		table: "TRACKS",
		jobID: jobID,
		columns: map[string]string{
			"ID": "string", "EVENT": "string", "CONTEXT": "json", "TAGS": "string", "RECEIVED_AT": "datetime", "UUID_TS": "datetime",
		},
		data: map[string]any{
			"ID":          id,
			"EVENT":       "clicked",
			"CONTEXT":     map[string]any{"name": name, "nested": map[string]any{"list": []any{1, 2}}},
			"TAGS":        []any{"x", "y"},
			"RECEIVED_AT": "2026-11-24T10:00:00.123Z",
		},
	}
}

func livePage(jobID int64) testEvent {
	return testEvent{
		table:   "PAGES",
		jobID:   jobID,
		columns: map[string]string{"ID": "string", "URL": "string", "RECEIVED_AT": "datetime"},
		data:    map[string]any{"ID": "page-" + strconv.FormatInt(jobID, 10), "URL": "https://example.com", "RECEIVED_AT": "2026-11-24T10:00:00.000Z"},
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
