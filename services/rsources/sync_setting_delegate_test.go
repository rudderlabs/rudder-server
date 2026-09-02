package rsources

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

// Component tests for the sync setting delegate that need no database.
//
// The database is deliberately a connector that counts dials and refuses to connect,
// so "this step did not reach the database" is an assertion on a counter rather than
// on an absence. The check-order test leans on that: steps 1 and 2 must answer
// ("", nil) even though any database access from this delegate would fail.
//
// The DB-backed half - the pinning upsert and the sweep - and the real constructor's
// migration and config bindings live in sync_setting_delegate_db_test.go.

const (
	testJobRunID  = "job-run-1"
	testTaskRunID = "task-run-1"
	testSourceID  = "src-1"
	testDestID    = "dst-1"
	testWorkspace = "ws-1"
)

func testStatKey() statKey {
	return statKey{
		jobRunId: testJobRunID,
		JobTargetKey: JobTargetKey{
			TaskRunID:     testTaskRunID,
			SourceID:      testSourceID,
			DestinationID: testDestID,
		},
	}
}

func abortedStatus(errorResponse string) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		JobID:         1,
		JobState:      jobsdb.Aborted.State,
		ErrorCode:     "422",
		ErrorResponse: json.RawMessage(errorResponse),
		WorkspaceId:   testWorkspace,
	}
}

// errDBUnreachable is what every database access of a probe delegate fails with.
var errDBUnreachable = errors.New("probe: the database must not be reached here")

// probeConnector is a database/sql connector that never connects and counts how many
// times it was asked to. A dial count of zero is the proof that a check-order step
// short-circuited before the pin lookup.
type probeConnector struct{ dials atomic.Int64 }

func (c *probeConnector) Connect(context.Context) (driver.Conn, error) {
	c.dials.Add(1)
	return nil, errDBUnreachable
}
func (c *probeConnector) Driver() driver.Driver { return probeDriver{} }

type probeDriver struct{}

func (probeDriver) Open(string) (driver.Conn, error) { return nil, errDBUnreachable }

// probeDelegate is a delegate wired to an unreachable database.
//
// It goes through newDelegate - the same field wiring NewSyncSettingDelegate uses -
// rather than rebuilding the struct, so a field added to the component cannot silently
// stay zero here. Only the database differs; NewSyncSettingDelegate itself needs a real
// one and is exercised in sync_setting_delegate_db_test.go.
type probeDelegate struct {
	*syncSettingDelegate
	conf      *config.Config
	stats     *memstats.Store
	log       *capturingLogger
	connector *probeConnector
}

func (p *probeDelegate) dials() int64 { return p.connector.dials.Load() }

// counter reads a memstats counter, returning 0 when it was never emitted.
func (p *probeDelegate) counter(name string, tags map[string]string) int {
	m := p.stats.Get(name, tags)
	if m == nil {
		return 0
	}
	return int(m.LastValue())
}

func (p *probeDelegate) captureTags() map[string]string {
	return map[string]string{
		"sourceId": testSourceID, "destinationId": testDestID, "workspaceId": testWorkspace,
	}
}

func newProbeDelegate(t *testing.T) *probeDelegate {
	t.Helper()
	conf := config.New()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	connector := &probeConnector{}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	log := newCapturingLogger()

	return &probeDelegate{
		syncSettingDelegate: newDelegate(conf, log, statsStore, db),
		conf:                conf,
		stats:               statsStore,
		log:                 log,
		connector:           connector,
	}
}

// pin seeds the cache the way a database read would - a row that was just created, so
// it gets the full maxAge as its TTL - so that step 3 answers without a round trip.
func (p *probeDelegate) pin(jobRunID string, store bool) {
	p.cacheDecision(jobRunID, store, time.Now(), p.maxAge.Load())
}

// cached is the cache lookup the delegate itself no longer needs: a hit returns the
// entry, a miss (or an expired entry) returns nil.
func (p *probeDelegate) cached(jobRunID string) *syncSettingEntry {
	return p.decisions.Get(jobRunID)
}

// configPushed marks the first backend-config push as having landed, so that
// awaitConfig does not block. Nothing is indexed: a probe delegate that reaches the
// pin lookup is a test failure, not a decision to compute.
func (p *probeDelegate) configPushed() {
	p.configLoadedOnce.Do(func() { close(p.configLoaded) })
}

// TestSyncSettingDelegateCheckOrder walks the five steps of the check order. Every case
// asserts the answer AND that no step before the one under test reached the pin cache
// or the database.
func TestSyncSettingDelegateCheckOrder(t *testing.T) {
	const boom = `{"response":"boom"}`

	tests := []struct {
		name          string
		setup         func(*probeDelegate)
		errorResponse string
		wantText      string
		// alsoAssert carries the extra evidence a row needs to pin its position in
		// the order rather than just its answer.
		alsoAssert func(*testing.T, *probeDelegate)
	}{
		{
			name:          "step 1 a nil error response never reaches the pin",
			errorResponse: "",
		},
		{
			name:          "step 1 the empty-object envelope never reaches the pin",
			errorResponse: `{}`,
		},
		{
			name:          "step 1 wins over the global flag being on",
			setup:         func(p *probeDelegate) { p.conf.Set(captureErrorDetailKey, true) },
			errorResponse: `{}`,
		},
		{
			name:          "step 2 the global flag being off never reaches the pin",
			errorResponse: boom,
		},
		{
			name: "step 2 an explicit false never reaches the pin",
			setup: func(p *probeDelegate) {
				p.conf.Set(captureErrorDetailKey, false)
				// A pinned true must not rescue a globally disabled process.
				p.pin(testJobRunID, true)
			},
			errorResponse: boom,
		},
		{
			name: "step 3 a pinned false stops before the blacklist and the text",
			setup: func(p *probeDelegate) {
				p.conf.Set(captureErrorDetailKey, true)
				p.conf.Set(blockedConnectionsKey, []string{testSourceID + ":" + testDestID})
				p.pin(testJobRunID, false)
			},
			errorResponse: boom,
			alsoAssert: func(t *testing.T, p *probeDelegate) {
				// The connection is blacklisted too, so if step 4 ran before step 3 the
				// record would have been counted as suppressed. A run that is simply
				// not capturing is not a suppression and must not inflate that counter.
				require.Zero(t, p.counter(rsourcesErrorSuppressed,
					map[string]string{"scope": blockedScopeConnection}))
				require.Empty(t, p.log.blockedWarns())
			},
		},
		{
			name: "step 4 a blacklisted connection loses the text",
			setup: func(p *probeDelegate) {
				p.conf.Set(captureErrorDetailKey, true)
				p.conf.Set(blockedConnectionsKey, []string{testSourceID + ":" + testDestID})
				p.pin(testJobRunID, true)
			},
			errorResponse: boom,
		},
		{
			name: "step 4 a blacklisted workspace loses the text",
			setup: func(p *probeDelegate) {
				p.conf.Set(captureErrorDetailKey, true)
				p.conf.Set(blockedWorkspacesKey, []string{testWorkspace})
				p.pin(testJobRunID, true)
			},
			errorResponse: boom,
		},
		{
			name: "step 5 the happy path unwraps the message",
			setup: func(p *probeDelegate) {
				p.conf.Set(captureErrorDetailKey, true)
				p.pin(testJobRunID, true)
			},
			errorResponse: boom,
			wantText:      "boom",
		},
		{
			name: "step 5 clips before it sanitises",
			setup: func(p *probeDelegate) {
				p.conf.Set(captureErrorDetailKey, true)
				p.conf.Set(maxErrorLengthKey, 4)
				p.pin(testJobRunID, true)
			},
			// The message is "abc\x00defghij". The cap takes the first four bytes,
			// "abc\x00", and the NUL is only stripped afterwards - sanitisation is
			// shrinking, which is what makes it safe to run after the cap.
			errorResponse: `{"reason":"abc` + "\\u0000" + `defghij"}`,
			wantText:      "abc",
		},
		{
			name: "step 5 an unrecognised envelope yields no text and no error",
			setup: func(p *probeDelegate) {
				p.conf.Set(captureErrorDetailKey, true)
				p.pin(testJobRunID, true)
			},
			errorResponse: `{"firstAttemptedAt":"2026-08-20T00:00:00.000Z"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newProbeDelegate(t)
			// The first config push is marked as landed but nothing is indexed, so a
			// step that reaches the pin lookup sails past awaitConfig straight into the
			// unreachable database. A nil error below is therefore itself proof that
			// the step under test short-circuited, for every case whose cache was not
			// seeded by setup.
			p.configPushed()
			if tc.setup != nil {
				tc.setup(p)
			}
			pinBefore := p.cached(testJobRunID)

			text, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(tc.errorResponse))

			require.NoError(t, err)
			require.Equal(t, tc.wantText, text)
			require.Zero(t, p.dials(), "this step must not reach the database")
			require.True(t, pinBefore == p.cached(testJobRunID),
				"no step of the check order may replace the run's cached decision")
			if tc.alsoAssert != nil {
				tc.alsoAssert(t, p)
			}
		})
	}
}

// TestSyncSettingDelegatePropagatesDatabaseErrors pins the "never swallow" half of the
// contract: once step 3 is reached, a database failure is an error, not an empty text.
func TestSyncSettingDelegatePropagatesDatabaseErrors(t *testing.T) {
	p := newProbeDelegate(t)
	p.configPushed()
	p.conf.Set(captureErrorDetailKey, true)

	text, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))

	require.Error(t, err, "a database failure must never be reported as an empty capture")
	require.ErrorIs(t, err, errDBUnreachable)
	require.Empty(t, text)
	require.Positive(t, p.dials(), "the pin lookup must actually have tried the database")
	require.Nil(t, p.cached(testJobRunID), "a failed lookup must not cache anything")
}

// TestSyncSettingDelegateWaitsForTheFirstConfigPush pins the startup-window guard: a
// record arriving before the first backend-config push must not pin the run against an
// empty index.
func TestSyncSettingDelegateWaitsForTheFirstConfigPush(t *testing.T) {
	p := newProbeDelegate(t)
	p.conf.Set(captureErrorDetailKey, true)
	// No configPushed() here: the first push has not landed.

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	text, err := p.GetErrorResponse(ctx, testStatKey(), abortedStatus(`{"response":"boom"}`))

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, text)
	require.Zero(t, p.dials(), "no pin may be computed against an empty connection index")
	require.Nil(t, p.cached(testJobRunID))
}

// TestSyncSettingDelegateEmptyErrorResponse pins the shapes the pipeline writes for "no detail recorded".
func TestSyncSettingDelegateEmptyErrorResponse(t *testing.T) {
	empty := []string{"", "   ", "\n", `''`, `{}`, "  {}  ", "\t{}\n", " '' "}
	for _, s := range empty {
		require.True(t, emptyErrorResponse(json.RawMessage(s)), "%q must be treated as empty", s)
	}
	require.True(t, emptyErrorResponse(nil))

	notEmpty := []string{`{"response":"boom"}`, `{"a":1}`, `[]`, `null`, `""`, `{ }`, `0`}
	for _, s := range notEmpty {
		require.False(t, emptyErrorResponse(json.RawMessage(s)), "%q must not be treated as empty", s)
	}
}

// TestSyncSettingDelegateBlockedPredicate pins the operator storm blacklist lookup.
func TestSyncSettingDelegateBlockedPredicate(t *testing.T) {
	tests := []struct {
		name               string
		blockedConnections []string
		blockedWorkspaces  []string
		sourceID           string
		destinationID      string
		workspaceID        string
		wantScope          string
		wantBlocked        bool
	}{
		{
			name:               "the connection key is the server side sourceID:destinationID",
			blockedConnections: []string{"src-1:dst-1"},
			sourceID:           "src-1", destinationID: "dst-1", workspaceID: "ws-1",
			wantScope: blockedScopeConnection, wantBlocked: true,
		},
		{
			name:               "a different destination on the same source is not blocked",
			blockedConnections: []string{"src-1:dst-1"},
			sourceID:           "src-1", destinationID: "dst-2", workspaceID: "ws-1",
		},
		{
			name:               "a different source on the same destination is not blocked",
			blockedConnections: []string{"src-1:dst-1"},
			sourceID:           "src-2", destinationID: "dst-1", workspaceID: "ws-1",
		},
		{
			name:              "workspaces are blocked on their own key",
			blockedWorkspaces: []string{"ws-1"},
			sourceID:          "src-1", destinationID: "dst-1", workspaceID: "ws-1",
			wantScope: blockedScopeWorkspace, wantBlocked: true,
		},
		{
			name:              "another workspace is not blocked",
			blockedWorkspaces: []string{"ws-1"},
			sourceID:          "src-1", destinationID: "dst-1", workspaceID: "ws-2",
		},
		{
			name:              "an empty workspace id never matches an empty blacklist entry",
			blockedWorkspaces: []string{""},
			sourceID:          "src-1", destinationID: "dst-1", workspaceID: "",
		},
		{
			name:     "nothing is blocked by default",
			sourceID: "src-1", destinationID: "dst-1", workspaceID: "ws-1",
		},
		{
			name:               "the connection scope wins when both match",
			blockedConnections: []string{"src-1:dst-1"},
			blockedWorkspaces:  []string{"ws-1"},
			sourceID:           "src-1", destinationID: "dst-1", workspaceID: "ws-1",
			wantScope: blockedScopeConnection, wantBlocked: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newProbeDelegate(t)
			if tc.blockedConnections != nil {
				p.conf.Set(blockedConnectionsKey, tc.blockedConnections)
			}
			if tc.blockedWorkspaces != nil {
				p.conf.Set(blockedWorkspacesKey, tc.blockedWorkspaces)
			}
			scope, blocked := p.blocked(tc.sourceID, tc.destinationID, tc.workspaceID)
			require.Equal(t, tc.wantBlocked, blocked)
			require.Equal(t, tc.wantScope, scope)
		})
	}
}

// connectionsPayload builds a backend-config push carrying the given connections,
// grouped by workspace, with the capture flag at the path the delegate reads.
func connectionsPayload(workspaces map[string][]backendconfig.Connection) map[string]backendconfig.ConfigT {
	out := make(map[string]backendconfig.ConfigT, len(workspaces))
	for workspaceID, conns := range workspaces {
		indexed := make(map[string]backendconfig.Connection, len(conns))
		for i, c := range conns {
			indexed[fmt.Sprintf("conn-%s-%d", workspaceID, i)] = c
		}
		out[workspaceID] = backendconfig.ConfigT{WorkspaceID: workspaceID, Connections: indexed}
	}
	return out
}

// connectionWith builds a connection whose config carries `enabled` at
// config.source.syncSettings.errorDetailsConfig.enabled.
func connectionWith(sourceID, destinationID string, enabled any) backendconfig.Connection {
	return backendconfig.Connection{
		SourceID:      sourceID,
		DestinationID: destinationID,
		Enabled:       true,
		Config: map[string]any{
			"source": map[string]any{
				"syncSettings": map[string]any{
					"errorDetailsConfig": map[string]any{"enabled": enabled},
				},
			},
		},
	}
}

// TestSyncSettingDelegateIndexConnections pins how the delegate reads the control plane.
func TestSyncSettingDelegateIndexConnections(t *testing.T) {
	t.Run("the flag is read from config.source.syncSettings.errorDetailsConfig.enabled", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.indexConnections(connectionsPayload(map[string][]backendconfig.Connection{
			"ws-a": {
				connectionWith("src-on", "dst-on", true),
				connectionWith("src-off", "dst-off", false),
			},
		}))
		require.True(t, p.connectionEnabled("src-on", "dst-on"))
		require.False(t, p.connectionEnabled("src-off", "dst-off"))
	})

	t.Run("connections from different workspaces land in one index", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.indexConnections(connectionsPayload(map[string][]backendconfig.Connection{
			"ws-a": {connectionWith("src-a", "dst-a", true)},
			"ws-b": {connectionWith("src-b", "dst-b", true), connectionWith("src-b2", "dst-b2", false)},
		}))
		require.True(t, p.connectionEnabled("src-a", "dst-a"), "workspace a's connection")
		require.True(t, p.connectionEnabled("src-b", "dst-b"), "workspace b's connection")
		require.False(t, p.connectionEnabled("src-b2", "dst-b2"))
		require.Len(t, p.connections, 3)
	})

	t.Run("an unknown connection resolves false", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.indexConnections(connectionsPayload(map[string][]backendconfig.Connection{
			"ws-a": {connectionWith("src-a", "dst-a", true)},
		}))
		require.False(t, p.connectionEnabled("src-unknown", "dst-unknown"))
		require.False(t, p.connectionEnabled("src-a", "dst-other"), "the destination is part of the key")
		require.False(t, p.connectionEnabled("src-other", "dst-a"), "the source is part of the key")
	})

	t.Run("a non-boolean or missing value resolves false", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.indexConnections(map[string]backendconfig.ConfigT{
			"ws-a": {
				WorkspaceID: "ws-a",
				Connections: map[string]backendconfig.Connection{
					"string-true": connectionWith("src-str", "dst-str", "true"),
					"number-one":  connectionWith("src-num", "dst-num", float64(1)),
					"nil-value":   connectionWith("src-nil", "dst-nil", nil),
					"no-config":   {SourceID: "src-none", DestinationID: "dst-none"},
					"shallow": {
						SourceID: "src-shallow", DestinationID: "dst-shallow",
						Config: map[string]any{"source": map[string]any{"syncSettings": true}},
					},
				},
			},
		})
		for _, c := range [][2]string{
			{"src-str", "dst-str"},
			{"src-num", "dst-num"},
			{"src-nil", "dst-nil"},
			{"src-none", "dst-none"},
			{"src-shallow", "dst-shallow"},
		} {
			require.False(t, p.connectionEnabled(c[0], c[1]), "%s:%s must fail closed", c[0], c[1])
		}
		require.Len(t, p.connections, 5, "every connection is still indexed, just as false")
	})

	t.Run("the index is rebuilt wholesale so a removed connection stops resolving true", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.indexConnections(connectionsPayload(map[string][]backendconfig.Connection{
			"ws-a": {connectionWith("src-a", "dst-a", true), connectionWith("src-gone", "dst-gone", true)},
		}))
		require.True(t, p.connectionEnabled("src-gone", "dst-gone"))

		// second push: the connection was deleted in the control plane
		p.indexConnections(connectionsPayload(map[string][]backendconfig.Connection{
			"ws-a": {connectionWith("src-a", "dst-a", true)},
		}))
		require.False(t, p.connectionEnabled("src-gone", "dst-gone"),
			"a merged update would leave a deleted connection answering true forever")
		require.True(t, p.connectionEnabled("src-a", "dst-a"))
		require.Len(t, p.connections, 1)

		// a whole workspace disappearing empties its half of the index
		p.indexConnections(map[string]backendconfig.ConfigT{})
		require.False(t, p.connectionEnabled("src-a", "dst-a"))
		require.Empty(t, p.connections)
	})

	t.Run("the first push unblocks awaitConfig and later pushes do not panic", func(t *testing.T) {
		p := newProbeDelegate(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, p.awaitConfig(ctx), "before the first push the wait honours the context")

		for range 3 {
			p.indexConnections(connectionsPayload(map[string][]backendconfig.Connection{
				"ws-a": {connectionWith("src-a", "dst-a", true)},
			}))
		}
		require.NoError(t, p.awaitConfig(ctx),
			"after the first push the wait is a closed-channel read, even with a dead context")
	})

	t.Run("an empty push still counts as the first push", func(t *testing.T) {
		// A tenant with no rETL connections must not leave every miss blocked forever.
		p := newProbeDelegate(t)
		p.indexConnections(map[string]backendconfig.ConfigT{})
		require.NoError(t, p.awaitConfig(context.Background()))
	})
}

// TestSyncSettingDelegateConfigSubscriberRoutine pins the subscription half: pushes are indexed, a payload
// of the wrong shape is reported and skipped, and a closed channel ends the routine.
func TestSyncSettingDelegateConfigSubscriberRoutine(t *testing.T) {
	t.Run("pushes are indexed until the channel closes", func(t *testing.T) {
		p := newProbeDelegate(t)
		ch := make(chan pubsub.DataEvent)
		p.configCh = ch
		done := make(chan error, 1)
		go func() { done <- p.ConfigSubscriberRoutine(context.Background()) }()

		ch <- pubsub.DataEvent{
			Topic: string(backendconfig.TopicProcessConfig),
			Data: connectionsPayload(map[string][]backendconfig.Connection{
				"ws-a": {connectionWith("src-a", "dst-a", true)},
			}),
		}
		// The next send is only accepted after the previous one was fully handled.
		ch <- pubsub.DataEvent{
			Topic: string(backendconfig.TopicProcessConfig),
			Data: connectionsPayload(map[string][]backendconfig.Connection{
				"ws-a": {connectionWith("src-a", "dst-a", false)},
			}),
		}
		close(ch)
		require.NoError(t, <-done)
		require.False(t, p.connectionEnabled("src-a", "dst-a"), "the last push wins")
	})

	t.Run("a payload of the wrong shape is reported and does not clobber the index", func(t *testing.T) {
		p := newProbeDelegate(t)
		ch := make(chan pubsub.DataEvent)
		p.configCh = ch
		done := make(chan error, 1)
		go func() { done <- p.ConfigSubscriberRoutine(context.Background()) }()

		ch <- pubsub.DataEvent{
			Topic: string(backendconfig.TopicProcessConfig),
			Data: connectionsPayload(map[string][]backendconfig.Connection{
				"ws-a": {connectionWith("src-a", "dst-a", true)},
			}),
		}
		ch <- pubsub.DataEvent{Topic: string(backendconfig.TopicProcessConfig), Data: "not a config map"}
		close(ch)
		require.NoError(t, <-done)

		require.True(t, p.connectionEnabled("src-a", "dst-a"), "the last good index must survive")
		require.Len(t, p.log.errorsWith("rsources sync settings: unexpected backend config payload"), 1)
	})
}

// TestSyncSettingDelegateTelemetry pins the counters the delegate emits: one per call, tagged with
// identifiers only.
func TestSyncSettingDelegateTelemetry(t *testing.T) {
	t.Run("a capture emits one captured count and no clip", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.configPushed()
		p.conf.Set(captureErrorDetailKey, true)
		p.pin(testJobRunID, true)

		for i := 1; i <= 3; i++ {
			text, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
			require.Equal(t, "boom", text)
			require.Equal(t, i, p.counter(rsourcesErrorCaptured, p.captureTags()),
				"one captured count per call, not per publish")
		}
		require.Equal(t, 0, p.counter(rsourcesErrorClipped, p.captureTags()))
		require.Equal(t, 0, p.counter(rsourcesErrorSuppressed, map[string]string{"scope": blockedScopeConnection}))
	})

	t.Run("a clipped capture is counted on both counters", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.configPushed()
		p.conf.Set(captureErrorDetailKey, true)
		p.conf.Set(maxErrorLengthKey, 16)
		p.pin(testJobRunID, true)

		text, err := p.GetErrorResponse(context.Background(), testStatKey(),
			abortedStatus(`{"response":"`+strings.Repeat("x", 100)+`"}`))
		require.NoError(t, err)
		require.Len(t, text, 16)
		require.Equal(t, 1, p.counter(rsourcesErrorCaptured, p.captureTags()))
		require.Equal(t, 1, p.counter(rsourcesErrorClipped, p.captureTags()))
	})

	t.Run("a misconfigured cap is counted as clipped and captures nothing", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.configPushed()
		p.conf.Set(captureErrorDetailKey, true)
		p.conf.Set(maxErrorLengthKey, 0)
		p.pin(testJobRunID, true)

		text, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
		require.NoError(t, err)
		require.Empty(t, text, "a non positive cap must fail closed")
		require.Equal(t, 0, p.counter(rsourcesErrorCaptured, p.captureTags()))
		require.Equal(t, 1, p.counter(rsourcesErrorClipped, p.captureTags()),
			"the operator has to be able to see the misconfiguration")
	})

	t.Run("an unrecognised envelope counts nothing at all", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.configPushed()
		p.conf.Set(captureErrorDetailKey, true)
		p.pin(testJobRunID, true)

		text, err := p.GetErrorResponse(context.Background(), testStatKey(),
			abortedStatus(`{"firstAttemptedAt":"2026-08-20T00:00:00.000Z"}`))
		require.NoError(t, err)
		require.Empty(t, text)
		// The suppression counters are created up front, so the series exist from
		// construction; what must not exist is a count on any of them.
		for _, m := range p.stats.GetAll() {
			require.Zerof(t, m.Value, "%s%v was counted, but nothing was captured, clipped or suppressed",
				m.Name, m.Tags)
		}
	})

	t.Run("suppression is counted per record on the scope tag", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.configPushed()
		p.conf.Set(captureErrorDetailKey, true)
		p.conf.Set(blockedConnectionsKey, []string{testSourceID + ":" + testDestID})
		p.conf.Set(blockedWorkspacesKey, []string{testWorkspace})
		p.pin(testJobRunID, true)

		for range 4 {
			text, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
			require.Empty(t, text)
		}
		// A different connection in the same blacklisted workspace.
		wsOnly := testStatKey()
		wsOnly.SourceID = "src-other"
		text, err := p.GetErrorResponse(context.Background(), wsOnly, abortedStatus(`{"response":"boom"}`))
		require.NoError(t, err)
		require.Empty(t, text)

		require.Equal(t, 4, p.counter(rsourcesErrorSuppressed, map[string]string{"scope": blockedScopeConnection}))
		require.Equal(t, 1, p.counter(rsourcesErrorSuppressed, map[string]string{"scope": blockedScopeWorkspace}))
		require.Equal(t, 0, p.counter(rsourcesErrorCaptured, p.captureTags()))
	})

	t.Run("no counter tag ever carries the message body", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.configPushed()
		p.conf.Set(captureErrorDetailKey, true)
		p.pin(testJobRunID, true)

		const secret = "Invalid API key: sk-live-abcdef"
		text, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"`+secret+`"}`))
		require.NoError(t, err)
		require.Equal(t, secret, text, "the message must still reach the caller")

		for _, m := range p.stats.GetAll() {
			for name, v := range m.Tags {
				require.NotContains(t, v, "sk-live", "metric %s leaked the body on tag %s", m.Name, name)
			}
		}
	})
}

// TestSyncSettingDelegateSuppressionWarnThrottle pins that the blacklist warning does not scale with the
// storm it is reporting.
func TestSyncSettingDelegateSuppressionWarnThrottle(t *testing.T) {
	newBlockedDelegate := func(t *testing.T) *probeDelegate {
		t.Helper()
		p := newProbeDelegate(t)
		p.configPushed()
		p.conf.Set(captureErrorDetailKey, true)
		p.conf.Set(blockedConnectionsKey, []string{testSourceID + ":" + testDestID})
		p.conf.Set(blockedWorkspacesKey, []string{testWorkspace})
		return p
	}

	t.Run("one warning per job run and scope, however large the storm", func(t *testing.T) {
		p := newBlockedDelegate(t)
		p.pin(testJobRunID, true)

		for range 500 {
			_, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
		}
		warns := p.log.blockedWarns()
		require.Len(t, warns, 1, "the log must not scale with the storm")
		require.Equal(t, blockedScopeConnection, warns[0].fieldMap()["reason"])
		require.Equal(t, testJobRunID, warns[0].fieldMap()["job_run_id"])
		require.Equal(t, testSourceID, warns[0].fieldMap()["sourceId"])
		require.Equal(t, testDestID, warns[0].fieldMap()["destinationId"])
		require.Equal(t, testWorkspace, warns[0].fieldMap()["workspaceId"])
		require.Equal(t, 500, p.counter(rsourcesErrorSuppressed, map[string]string{"scope": blockedScopeConnection}),
			"the counter, unlike the log, is per record")
		require.Len(t, p.warned, 1)
	})

	t.Run("a second scope on the same job run warns once of its own", func(t *testing.T) {
		p := newBlockedDelegate(t)
		p.pin(testJobRunID, true)

		wsOnly := testStatKey() // same job run, different connection: workspace scope
		wsOnly.SourceID = "src-other"
		for range 10 {
			_, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
			_, err = p.GetErrorResponse(context.Background(), wsOnly, abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
		}
		warns := p.log.blockedWarns()
		require.Len(t, warns, 2)
		require.ElementsMatch(t,
			[]any{blockedScopeConnection, blockedScopeWorkspace},
			[]any{warns[0].fieldMap()["reason"], warns[1].fieldMap()["reason"]})
		require.Len(t, p.warned, 2)
	})

	t.Run("a different job run warns on its own", func(t *testing.T) {
		p := newBlockedDelegate(t)
		other := testStatKey()
		other.jobRunId = "job-run-2"
		p.pin(testJobRunID, true)
		p.pin(other.jobRunId, true)

		for range 5 {
			_, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
			_, err = p.GetErrorResponse(context.Background(), other, abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
		}
		require.Len(t, p.log.blockedWarns(), 2)
	})

	t.Run("the cleanup sweep resets the throttle so a still blocked run warns again", func(t *testing.T) {
		p := newBlockedDelegate(t)
		p.pin(testJobRunID, true)

		for range 20 {
			_, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
		}
		require.Len(t, p.log.blockedWarns(), 1)

		// A sweep that deletes nothing still has to reset the throttle, otherwise a
		// connection blacklisted for days goes quiet forever after its first line.
		p.resetWarnThrottle()
		require.Empty(t, p.warned)

		for range 20 {
			_, err := p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
			require.NoError(t, err)
		}
		require.Len(t, p.log.blockedWarns(), 2)
	})

	t.Run("concurrent suppressions of one job run still warn exactly once", func(t *testing.T) {
		p := newBlockedDelegate(t)
		p.pin(testJobRunID, true)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 16 {
			wg.Go(func() {
				<-start
				for range 20 {
					_, _ = p.GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"boom"}`))
				}
			})
		}
		close(start)
		wg.Wait()
		require.Len(t, p.log.blockedWarns(), 1)
		require.Equal(t, 16*20, p.counter(rsourcesErrorSuppressed, map[string]string{"scope": blockedScopeConnection}))
	})
}

// TestSyncSettingDelegateCacheDecision pins how a decision ages out of memory.
//
// There is no sweep over the cache any more: each entry is given the row's own
// remaining lifetime as its TTL, so it disappears from memory at the moment the row
// itself becomes eligible for the cleanup DELETE. A row already past maxAge - which
// every startup loadAll can see, since another pod may not have swept yet - must not be
// cached at all, or a pod could keep answering from a decision that no longer exists.
func TestSyncSettingDelegateCacheDecision(t *testing.T) {
	const maxAge = time.Hour

	t.Run("a row inside the retention window is cached", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.cacheDecision("fresh", true, time.Now(), maxAge)
		entry := p.cached("fresh")
		require.NotNil(t, entry)
		require.True(t, entry.storeErrorResponses)

		p.cacheDecision("fresh-false", false, time.Now().Add(-maxAge/2), maxAge)
		entry = p.cached("fresh-false")
		require.NotNil(t, entry, "half way through the window the row is still live")
		require.False(t, entry.storeErrorResponses, "a decision of false is cached, not treated as a miss")
	})

	t.Run("a row already past maxAge is never cached", func(t *testing.T) {
		p := newProbeDelegate(t)
		p.cacheDecision("expired", true, time.Now().Add(-2*maxAge), maxAge)
		require.Nil(t, p.cached("expired"))

		// Exactly at the cutoff the remaining lifetime is zero, which is not a TTL.
		p.cacheDecision("borderline", true, time.Now().Add(-maxAge), maxAge)
		require.Nil(t, p.cached("borderline"))
	})

	t.Run("the entry expires with the row rather than on its own clock", func(t *testing.T) {
		p := newProbeDelegate(t)
		// A row created maxAge-ago-plus-a-moment: it has only that moment left in the
		// table, so it may only have that moment in memory.
		p.cacheDecision("expiring", true, time.Now().Add(-maxAge+50*time.Millisecond), maxAge)
		require.NotNil(t, p.cached("expiring"), "the row is still inside the window here")
		require.Eventually(t, func() bool { return p.cached("expiring") == nil },
			30*time.Second, 5*time.Millisecond,
			"the entry must not outlive the row's own retention")
	})
}

// TestStaticSyncSettingDelegate pins the canned delegate the tests and the
// feature-disabled deployments use.
func TestStaticSyncSettingDelegate(t *testing.T) {
	t.Run("it answers with its canned text", func(t *testing.T) {
		text, err := NewStaticSyncSettingDelegate("canned", nil).
			GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"ignored"}`))
		require.NoError(t, err)
		require.Equal(t, "canned", text)
	})

	t.Run("it answers with its canned error, which the collector must propagate", func(t *testing.T) {
		sentinel := errors.New("canned failure")
		text, err := NewStaticSyncSettingDelegate("unused", sentinel).
			GetErrorResponse(context.Background(), testStatKey(), abortedStatus(`{"response":"x"}`))
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, "unused", text)
	})
}

// capturedLog is one structured log line the delegate emitted.
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

// capturingLogger records the structured log lines the component emits so tests can
// assert both their contents and, crucially, what they must never contain. It is
// mutex-guarded because the delegate is exercised concurrently under -race.
type capturingLogger struct {
	logger.Logger
	mu     sync.Mutex
	infos  []capturedLog
	warns  []capturedLog
	errors []capturedLog
}

func newCapturingLogger() *capturingLogger { return &capturingLogger{Logger: logger.NOP} }

func (l *capturingLogger) Infon(msg string, fields ...logger.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, capturedLog{msg: msg, fields: fields})
}

func (l *capturingLogger) Warnn(msg string, fields ...logger.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, capturedLog{msg: msg, fields: fields})
}

func (l *capturingLogger) Errorn(msg string, fields ...logger.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, capturedLog{msg: msg, fields: fields})
}

func (l *capturingLogger) Child(string) logger.Logger { return l }

// blockedWarnMsg is the only warning the delegate emits - the throttled blacklist
// suppression line.
const blockedWarnMsg = "rsources: error capture blocked"

// blockedWarns and errorsWith return the recorded lines of interest. Both read the
// slices under the lock so that they are safe to call after a concurrent exercise of
// the delegate.
func (l *capturingLogger) blockedWarns() []capturedLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	return withMessage(l.warns, blockedWarnMsg)
}

func (l *capturingLogger) errorsWith(msg string) []capturedLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	return withMessage(l.errors, msg)
}

func withMessage(entries []capturedLog, msg string) []capturedLog {
	var out []capturedLog
	for _, e := range entries {
		if e.msg == msg {
			out = append(out, e)
		}
	}
	return out
}

// TestSyncSettingDelegateShutdown covers the two shutdown hazards of a component whose
// routines are started on somebody else's errgroup: a routine registering itself after
// Stop has begun waiting, and Stop having to outwait a whole cleanup interval.
func TestSyncSettingDelegateShutdown(t *testing.T) {
	t.Run("Stop returns without waiting out the cleanup interval", func(t *testing.T) {
		p := newProbeDelegate(t)
		// An interval no test would ever sit through: Stop must not be waiting on it.
		p.conf.Set(syncSettingsCleanupFrequencyKey, time.Hour)

		routineDone := make(chan struct{})
		go func() {
			defer close(routineDone)
			require.NoError(t, p.CleanupRoutine(context.Background()))
		}()

		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			p.Stop()
		}()

		select {
		case <-stopped:
		case <-time.After(30 * time.Second):
			t.Fatal("Stop did not return: it is waiting out the cleanup interval")
		}
		select {
		case <-routineDone:
		case <-time.After(30 * time.Second):
			t.Fatal("CleanupRoutine did not return after Stop")
		}
	})

	t.Run("a routine starting after Stop is refused instead of panicking", func(t *testing.T) {
		// Registering with the WaitGroup from inside the goroutine is the classic
		// "Add called concurrently with Wait" panic. Both routines must decline to run
		// once Stop has been observed, and must not touch the WaitGroup.
		p := newProbeDelegate(t)
		p.Stop()

		require.NoError(t, p.CleanupRoutine(context.Background()))
		require.NoError(t, p.ConfigSubscriberRoutine(context.Background()))
		require.NotPanics(t, p.Stop, "Stop must be safe to call more than once")
	})

	t.Run("the config routine returns on Stop even with the subscription still open", func(t *testing.T) {
		// The subscription is closed by pubsub when the CONSTRUCTOR's context is done.
		// A Stop that does not also cancel that context must still unblock the routine,
		// otherwise Stop's wg.Wait deadlocks.
		p := newProbeDelegate(t)
		p.configCh = make(pubsub.DataChannel) // never closed, never written

		done := make(chan struct{})
		go func() {
			defer close(done)
			require.NoError(t, p.ConfigSubscriberRoutine(context.Background()))
		}()

		p.Stop()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("ConfigSubscriberRoutine did not return on Stop")
		}
	})
}
