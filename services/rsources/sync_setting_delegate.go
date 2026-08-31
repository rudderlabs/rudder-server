package rsources

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

// SyncSettingDelegate is the single authority for the question "must the sync run
// `jobRunID` on connection {sourceID, destinationID} capture error responses?", and
// for producing the text to store when the answer is yes.
//
// It exists so that the failed-records collector - which runs on the router and batch
// router hot paths, once per status-update batch - holds no capture policy of its own.
// The collector asks; the delegate decides.
//
// The decision is *pinned per job run* in `rsources_sync_settings`, not re-read per
// record: server pods restart roughly every two hours and several pods share one
// postgres, so an in-memory-only pin would let one sync's records be split between two
// answers whenever a pod cycled or a config push landed mid-run.
type SyncSettingDelegate interface {
	// GetErrorResponse returns the error text to store for an aborted record, or ""
	// when capture is off for this run, connection or workspace.
	//
	// A database error is propagated, never swallowed: capture is part of the durable
	// failed-record row, and silently storing "" on a transient database problem would
	// produce a permanently wrong record with no signal anywhere.
	GetErrorResponse(ctx context.Context, key statKey, status *jobsdb.JobStatusT) (string, error)
}

// Config keys gating the capture of the final recorded error text for a failed
// record. All four are reloadable: capture is a beta-gated, storm-prone feature and
// operators must be able to turn it off or blacklist a noisy connection without a
// restart.
const (
	captureErrorDetailKey = "Rsources.failedKeys.captureErrorDetail"
	maxErrorLengthKey     = "Rsources.failedKeys.maxErrorLength"
	blockedConnectionsKey = "Rsources.failedKeys.blockedConnections"
	blockedWorkspacesKey  = "Rsources.failedKeys.blockedWorkspaces"
)

// defaultMaxErrorLength bounds a single captured message. It covers the p99 of real
// destination bodies and bounds the per-insert WAL volume of a failure storm; the
// column itself stays TEXT so that an oversized message is clipped in code rather
// than erroring the whole insert batch.
const defaultMaxErrorLength = 2048

// Error capture counters, tagged by {sourceId, destinationId, workspaceId} except
// for the suppression counter, which is tagged by {scope}.
const (
	rsourcesErrorCaptured   = "rsources_failed_records_error_captured"
	rsourcesErrorClipped    = "rsources_failed_records_error_clipped"
	rsourcesErrorSuppressed = "rsources_failed_records_error_suppressed"
)

// Blacklist scopes, used both as the suppression reason on the log line and as the
// `scope` tag of the suppression counter.
const (
	blockedScopeConnection = "blockedConnection"
	blockedScopeWorkspace  = "blockedWorkspace"
)

// syncSettingDelegate is the process-wide instance of the component. One per process:
// it owns a database connection, a backend-config subscription and a cleanup routine.
type syncSettingDelegate struct {
	log         logger.Logger
	statFactory stats.Stats
	conf        *config.Config
	db          *sql.DB

	// Reloadable knobs, read with .Load() at the point of use. Deliberately not
	// snapshotted per batch: a snapshot buys uniformity within one status-update
	// batch, which nothing depends on, at the cost of a struct copy on a hot path.
	enabled            config.ValueLoader[bool]
	maxErrorLength     config.ValueLoader[int]
	blockedConnections config.ValueLoader[[]string]
	blockedWorkspaces  config.ValueLoader[[]string]

	// maxAge is how long a pinned decision is kept. It bounds both the table and the
	// cache; a run still producing failed records after maxAge re-derives its decision
	// from the current config, which is an accepted outcome for a >24h sync.
	maxAge           config.ValueLoader[time.Duration]
	cleanupFrequency config.ValueLoader[time.Duration]

	// decisions caches rows of rsources_sync_settings. It only ever holds rows that
	// exist in the database - absence is never cached, so a pod that started before
	// another pod pinned a run adopts that pod's row on its first miss rather than
	// re-deriving the decision from its own view of the config.
	decisionsMu sync.RWMutex
	decisions   map[string]syncSettingEntry

	// connections is the {sourceID, destinationID} -> enabled index built from this
	// component's own backend-config subscription, rebuilt wholesale on every push.
	connectionsMu sync.RWMutex
	connections   map[connectionKey]bool
	// configLoaded is closed once the first backend-config push has been indexed, so
	// that a decision is never computed - and pinned for maxAge - against an empty
	// index during the startup window.
	configLoaded     chan struct{}
	configLoadedOnce sync.Once

	// warned throttles the blacklist suppression log to once per {jobRunID, scope}.
	// A storm is exactly when this fires, so per-record logging would be a
	// self-inflicted second storm. Reset by the cleanup routine.
	warnedMu sync.Mutex
	warned   map[suppressionKey]struct{}

	configCh pubsub.DataChannel

	// Shutdown state. stopped and wg are guarded by the same mutex so that a routine
	// starting concurrently with Stop can never call wg.Add after wg.Wait has been
	// entered - the WaitGroup misuse that panics at runtime. A routine that loses that
	// race simply does not run, which is what Stop asked for anyway.
	stopMu  sync.Mutex
	stopped bool
	stop    chan struct{}
	wg      sync.WaitGroup
}

// syncSettingEntry is one cached row. createdAt is the database's value, so that the
// local cache sweep uses the same clock and the same retention as the DELETE.
type syncSettingEntry struct {
	storeErrorResponses bool
	createdAt           time.Time
}

type connectionKey struct {
	sourceID, destinationID string
}

type suppressionKey struct {
	jobRunID string
	scope    string
}

var _ SyncSettingDelegate = (*syncSettingDelegate)(nil)

// NewSyncSettingDelegate builds the component: it opens its own database connection,
// creates its table if needed, loads every pinned decision into memory and subscribes
// to the backend config.
//
// ctx is the INSTANCE's lifetime, not a setup deadline. It is handed to
// backendConfig.Subscribe, and utils/pubsub closes the subscription when it is done -
// so a context with a short timeout would silently freeze the connection index a few
// seconds after boot, and every later first-miss would then stall in awaitConfig. Pass
// the same context the routines run under.
//
// The returned value still needs its routines started - see ConfigSubscriberRoutine
// and CleanupRoutine - and Stop called on shutdown.
func NewSyncSettingDelegate(
	ctx context.Context,
	conf *config.Config,
	log logger.Logger,
	statFactory stats.Stats,
	backendConfig backendconfig.BackendConfig,
) (*syncSettingDelegate, error) {
	if log == nil {
		log = logger.NOP
	}
	db, err := setupSyncSettingsDBConn(conf, statFactory)
	if err != nil {
		return nil, fmt.Errorf("rsources sync settings: db setup: %w", err)
	}
	d := &syncSettingDelegate{
		log:                log,
		statFactory:        statFactory,
		conf:               conf,
		db:                 db,
		enabled:            conf.GetReloadableBoolVar(false, captureErrorDetailKey),
		maxErrorLength:     conf.GetReloadableIntVar(defaultMaxErrorLength, 1, maxErrorLengthKey),
		blockedConnections: conf.GetReloadableStringSliceVar(nil, blockedConnectionsKey),
		blockedWorkspaces:  conf.GetReloadableStringSliceVar(nil, blockedWorkspacesKey),
		maxAge:             conf.GetReloadableDurationVar(24, time.Hour, syncSettingsMaxAgeKey),
		cleanupFrequency:   conf.GetReloadableDurationVar(1, time.Hour, syncSettingsCleanupFrequencyKey),
		decisions:          make(map[string]syncSettingEntry),
		connections:        make(map[connectionKey]bool),
		configLoaded:       make(chan struct{}),
		warned:             make(map[suppressionKey]struct{}),
		stop:               make(chan struct{}),
	}
	if err := d.setupTable(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("rsources sync settings: %w", err)
	}
	if err := d.loadAll(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("rsources sync settings: %w", err)
	}
	// Subscribed here rather than inside the routine so that a push landing between
	// construction and the routine's first iteration is not missed.
	d.configCh = backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	return d, nil
}

// GetErrorResponse implements SyncSettingDelegate.
//
// The check order is load-bearing, cheapest and most-selective first: the overwhelming
// majority of aborted records carry nothing worth storing, and none of them may reach
// the cache or the database.
func (d *syncSettingDelegate) GetErrorResponse(ctx context.Context, key statKey, status *jobsdb.JobStatusT) (string, error) {
	// 1. Nothing to capture. Checked on the raw envelope before anything else so that
	// a record with an empty error response costs one comparison.
	if emptyErrorResponse(status.ErrorResponse) {
		return "", nil
	}
	// 2. Feature off process-wide. No pin is read and, crucially, none is written:
	// turning the flag on must not find every live run already pinned to false.
	if !d.enabled.Load() {
		return "", nil
	}
	// 3. The run's pinned decision.
	store, err := d.storeErrorResponses(ctx, key)
	if err != nil {
		return "", err
	}
	if !store {
		return "", nil
	}
	// 4. Operator storm blacklist.
	if scope, blocked := d.blocked(key.SourceID, key.DestinationID, status.WorkspaceId); blocked {
		d.reportSuppressed(key, status.WorkspaceId, scope)
		return "", nil
	}
	// 5. Extract, clip, sanitise.
	text, clipped := captureErrorText(status.ErrorResponse, d.maxErrorLength.Load())
	d.reportCaptured(key, status.WorkspaceId, text != "", clipped)
	return text, nil
}

// emptyErrorResponse reports whether the raw envelope holds nothing at all. `”` and
// `{}` are the two shapes the pipeline writes for "no detail recorded"; surrounding
// whitespace is tolerated so that a `{}\n` shaped value short-circuits too.
func emptyErrorResponse(errorResponse json.RawMessage) bool {
	trimmed := bytes.TrimSpace(errorResponse)
	return len(trimmed) == 0 ||
		bytes.Equal(trimmed, []byte(`''`)) ||
		bytes.Equal(trimmed, []byte(`{}`))
}

// blocked reports whether the operator storm blacklist suppresses the message for
// this connection or workspace, along with the scope that matched.
//
// The connection key is the server-side `<sourceID>:<destinationID>` identity, not
// the control-plane connection id.
func (d *syncSettingDelegate) blocked(sourceID, destinationID, workspaceID string) (scope string, blocked bool) {
	if blockedConnections := d.blockedConnections.Load(); len(blockedConnections) > 0 &&
		slices.Contains(blockedConnections, sourceID+":"+destinationID) {
		return blockedScopeConnection, true
	}
	if blockedWorkspaces := d.blockedWorkspaces.Load(); workspaceID != "" && len(blockedWorkspaces) > 0 &&
		slices.Contains(blockedWorkspaces, workspaceID) {
		return blockedScopeWorkspace, true
	}
	return "", false
}

// reportCaptured counts one capture outcome. Prometheus aggregates, so a Count(1) per
// record is the cheapest correct thing to do here and removes the per-publish
// bookkeeping the collector used to carry.
//
// Counts, identifiers and the outcome only: a captured error may echo customer PII and
// occasionally a secret, so the message body is never logged or tagged.
func (d *syncSettingDelegate) reportCaptured(key statKey, workspaceID string, captured, clipped bool) {
	if !captured && !clipped {
		return
	}
	tags := stats.Tags{
		"sourceId":      key.SourceID,
		"destinationId": key.DestinationID,
		"workspaceId":   workspaceID,
	}
	if captured {
		d.statFactory.NewTaggedStat(rsourcesErrorCaptured, stats.CountType, tags).Count(1)
	}
	if clipped {
		d.statFactory.NewTaggedStat(rsourcesErrorClipped, stats.CountType, tags).Count(1)
	}
}

// reportSuppressed counts one blacklisted record and logs at most one line per
// {job run, scope} - the blacklist is reached for during a storm, and the log must not
// scale with the storm.
func (d *syncSettingDelegate) reportSuppressed(key statKey, workspaceID, scope string) {
	d.statFactory.NewTaggedStat(rsourcesErrorSuppressed, stats.CountType, stats.Tags{
		"scope": scope,
	}).Count(1)
	if !d.shouldWarn(key.jobRunId, scope) {
		return
	}
	d.log.Warnn("rsources: error capture blocked",
		obskit.SourceID(key.SourceID),
		obskit.DestinationID(key.DestinationID),
		obskit.WorkspaceID(workspaceID),
		logger.NewStringField("job_run_id", key.jobRunId),
		logger.NewStringField("reason", scope),
	)
}

func (d *syncSettingDelegate) shouldWarn(jobRunID, scope string) bool {
	k := suppressionKey{jobRunID: jobRunID, scope: scope}
	d.warnedMu.Lock()
	defer d.warnedMu.Unlock()
	if _, seen := d.warned[k]; seen {
		return false
	}
	d.warned[k] = struct{}{}
	return true
}

// Stop releases the component's resources and waits for the routines to return.
//
// It does not need the routines' context to have been cancelled first: closing d.stop
// wakes them out of their sleep, so Stop returns promptly rather than after a whole
// cleanup interval. Safe to call more than once.
func (d *syncSettingDelegate) Stop() {
	d.stopMu.Lock()
	if !d.stopped {
		d.stopped = true
		close(d.stop)
	}
	d.stopMu.Unlock()

	_ = d.db.Close()
	d.wg.Wait()
}

// routineStarted registers a background routine with the shutdown WaitGroup, reporting
// false if Stop has already run - in which case the routine must return immediately
// without touching the WaitGroup.
func (d *syncSettingDelegate) routineStarted() bool {
	d.stopMu.Lock()
	defer d.stopMu.Unlock()
	if d.stopped {
		return false
	}
	d.wg.Add(1)
	return true
}

// stopping reports whether Stop has been called.
func (d *syncSettingDelegate) stopping() bool {
	select {
	case <-d.stop:
		return true
	default:
		return false
	}
}
