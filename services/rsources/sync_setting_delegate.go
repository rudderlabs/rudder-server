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

	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
	db          *sql.DB

	// Reloadable knobs, read with .Load() at the point of use.
	enabled            config.ValueLoader[bool]
	maxErrorLength     config.ValueLoader[int]
	blockedConnections config.ValueLoader[[]string]
	blockedWorkspaces  config.ValueLoader[[]string]

	// maxAge is how long a pinned decision is kept. It bounds both the table and the
	// cache; a run still producing failed records after maxAge re-derives its decision
	// from the current config, which is an accepted outcome for a >24h sync.
	maxAge           config.ValueLoader[time.Duration]
	cleanupFrequency config.ValueLoader[time.Duration]

	// decisions caches rows of rsources_sync_settings, each expiring when the row
	// itself becomes eligible for the cleanup DELETE. It only ever holds rows that
	// exist in the database - absence is never cached, so a pod that started before
	// another pod pinned a run adopts that pod's row on its first miss rather than
	// deriving the decision from its own view of the config.
	decisions *cachettl.Cache[string, *syncSettingEntry]

	// connections is the {sourceID, destinationID} -> enabled index built from this
	// component's own backend-config subscription, rebuilt wholesale on every push.
	// captureStats holds that index's pre-created counters, so the hot path never
	// calls NewTaggedStat.
	connectionsMu sync.RWMutex
	connections   map[connectionKey]bool
	captureStats  map[captureStatKey]*captureCounters
	// configLoaded is closed once the first backend-config push has been indexed, so
	// that a decision is never computed - and pinned for maxAge - against an empty
	// index during the startup window.
	configLoaded     chan struct{}
	configLoadedOnce sync.Once

	// suppressedStats are the two possible suppression counters, one per scope. The
	// scope set is closed, so they are all created up front.
	suppressedStats map[string]stats.Counter

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

// syncSettingEntry is one cached row. Held by pointer so that a cache miss - which
// must go to the database - is distinguishable from a cached false.
type syncSettingEntry struct {
	storeErrorResponses bool
}

type connectionKey struct {
	sourceID, destinationID string
}

// captureStatKey is the identity of one capture counter series. It carries the
// workspace as well as the connection so that the tags can only ever be derived from
// the key - the eager and the lazy path cannot disagree about a series' labels.
type captureStatKey struct {
	connectionKey
	workspaceID string
}

// captureCounters are one connection's capture counters, created once per config push
// rather than per record: NewTaggedStat takes a lock and builds a tag map, and this
// runs on the router's status-update path for every aborted rETL record.
//
// Counts, identifiers and the outcome only: a captured error may echo customer PII and
// occasionally a secret, so the message body is never logged or tagged.
type captureCounters struct {
	captured stats.Counter
	clipped  stats.Counter
}

type suppressionKey struct {
	jobRunID string
	scope    string
}

var _ SyncSettingDelegate = (*syncSettingDelegate)(nil)

// newDelegate builds the in-memory half of the component: the config bindings, the
// caches and the shutdown plumbing, over an already-open database handle.
//
// Split from NewSyncSettingDelegate so that tests wanting a delegate over a fake
// database get the real field wiring instead of hand-rebuilding it - a copy that
// silently drifts the moment a field is added here.
func newDelegate(conf *config.Config, log logger.Logger, statFactory stats.Stats, db *sql.DB) *syncSettingDelegate {
	if log == nil {
		log = logger.NOP
	}
	return &syncSettingDelegate{
		log:                log,
		statFactory:        statFactory,
		db:                 db,
		enabled:            conf.GetReloadableBoolVar(false, captureErrorDetailKey),
		maxErrorLength:     conf.GetReloadableIntVar(defaultMaxErrorLength, 1, maxErrorLengthKey),
		blockedConnections: conf.GetReloadableStringSliceVar(nil, blockedConnectionsKey),
		blockedWorkspaces:  conf.GetReloadableStringSliceVar(nil, blockedWorkspacesKey),
		maxAge:             conf.GetReloadableDurationVar(24, time.Hour, syncSettingsMaxAgeKey),
		cleanupFrequency:   conf.GetReloadableDurationVar(1, time.Hour, syncSettingsCleanupFrequencyKey),
		decisions:          cachettl.New[string, *syncSettingEntry](cachettl.WithNoRefreshTTL),
		connections:        make(map[connectionKey]bool),
		captureStats:       make(map[captureStatKey]*captureCounters),
		configLoaded:       make(chan struct{}),
		warned:             make(map[suppressionKey]struct{}),
		stop:               make(chan struct{}),
		suppressedStats: map[string]stats.Counter{
			blockedScopeConnection: statFactory.NewTaggedStat(rsourcesErrorSuppressed, stats.CountType,
				stats.Tags{"scope": blockedScopeConnection}),
			blockedScopeWorkspace: statFactory.NewTaggedStat(rsourcesErrorSuppressed, stats.CountType,
				stats.Tags{"scope": blockedScopeWorkspace}),
		},
	}
}

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
	dsn, err := syncSettingsDSN(conf)
	if err != nil {
		return nil, fmt.Errorf("rsources sync settings: %w", err)
	}
	// Migration runs and finishes on its own pool before the serving one is opened, so
	// no connection of the serving pool is ever parked by the migrator.
	if err := migrateSyncSettings(conf, dsn); err != nil {
		return nil, fmt.Errorf("rsources sync settings: %w", err)
	}
	db, err := setupSyncSettingsDBConn(conf, dsn, statFactory)
	if err != nil {
		return nil, fmt.Errorf("rsources sync settings: db setup: %w", err)
	}
	d := newDelegate(conf, log, statFactory, db)
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

// emptyErrorResponse reports whether the raw envelope holds nothing at all.
//
// Empty, `”` and `{}` are the three shapes enumerated by the review that specified
// this check order; no producer of the `”` form exists in this repository, so it is
// carried on the reviewer's authority rather than on an observed writer. Surrounding
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
// record is the cheapest correct thing to do here.
func (d *syncSettingDelegate) reportCaptured(key statKey, workspaceID string, captured, clipped bool) {
	if !captured && !clipped {
		return
	}
	c := d.counters(captureStatKey{
		connectionKey: connectionKey{sourceID: key.SourceID, destinationID: key.DestinationID},
		workspaceID:   workspaceID,
	})
	if captured {
		c.captured.Count(1)
	}
	if clipped {
		c.clipped.Count(1)
	}
}

// counters returns the connection's counters, creating and memoising them on a miss.
//
// A miss happens for a connection this pod's config index does not carry - a run whose
// pin was adopted from another pod, for instance. That is exactly a storm's shape, so
// the miss must not call NewTaggedStat per record: it is memoised into the same map
// under the write lock.
func (d *syncSettingDelegate) counters(k captureStatKey) *captureCounters {
	d.connectionsMu.RLock()
	c, ok := d.captureStats[k]
	d.connectionsMu.RUnlock()
	if ok {
		return c
	}

	d.connectionsMu.Lock()
	defer d.connectionsMu.Unlock()
	if c, ok := d.captureStats[k]; ok { // lost the race to another record
		return c
	}
	c = newCaptureCounters(d.statFactory, k)
	d.captureStats[k] = c
	return c
}

// newCaptureCounters builds one series. The tags come from the key and nowhere else,
// so a counter created eagerly from the config push and one created lazily from a job
// status can never end up as two differently-labelled series for the same identity.
func newCaptureCounters(statFactory stats.Stats, k captureStatKey) *captureCounters {
	tags := stats.Tags{
		"sourceId":      k.sourceID,
		"destinationId": k.destinationID,
		"workspaceId":   k.workspaceID,
	}
	return &captureCounters{
		captured: statFactory.NewTaggedStat(rsourcesErrorCaptured, stats.CountType, tags),
		clipped:  statFactory.NewTaggedStat(rsourcesErrorClipped, stats.CountType, tags),
	}
}

// reportSuppressed counts one blacklisted record and logs at most one line per
// {job run, scope} - the blacklist is reached for during a storm, and the log must not
// scale with the storm.
func (d *syncSettingDelegate) reportSuppressed(key statKey, workspaceID, scope string) {
	d.suppressedStats[scope].Count(1)
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
//
// The database closes LAST: the routines run queries against it, so closing it while
// they are still in flight would fail them on the way out.
func (d *syncSettingDelegate) Stop() {
	d.stopMu.Lock()
	if !d.stopped {
		d.stopped = true
		close(d.stop)
	}
	d.stopMu.Unlock()

	d.wg.Wait()
	_ = d.db.Close()
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

// ConfigSubscriberRoutine consumes the component's backend-config subscription and
// rebuilds the connection index on every push.
//
// It returns when the subscription closes (utils/pubsub closes it when the context
// handed to Subscribe - the constructor's - is done), when ctx is cancelled, or when
// Stop is called. The last of those matters: without it, a Stop that does not also
// cancel the constructor's context would block forever in wg.Wait().
func (d *syncSettingDelegate) ConfigSubscriberRoutine(ctx context.Context) error {
	if !d.routineStarted() {
		return nil
	}
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-d.stop:
			return nil
		case data, open := <-d.configCh:
			if !open {
				return nil
			}
			workspaces, ok := data.Data.(map[string]backendconfig.ConfigT)
			if !ok {
				// TopicProcessConfig always carries map[string]ConfigT; a different
				// shape means the topic contract changed under us and every decision
				// from here on would silently be false.
				d.log.Errorn("rsources sync settings: unexpected backend config payload")
				continue
			}
			d.indexConnections(workspaces)
		}
	}
}

// indexConnections rebuilds the {sourceID, destinationID} -> enabled index wholesale.
//
// Rebuilt rather than merged so that a connection removed from the control plane
// disappears here too; a partial update would leave a deleted connection answering
// `true` forever. The index spans every workspace in the push - one process serves
// many workspaces, and the connection identity is already globally unique.
func (d *syncSettingDelegate) indexConnections(workspaces map[string]backendconfig.ConfigT) {
	index := make(map[connectionKey]bool)
	counters := make(map[captureStatKey]*captureCounters)
	for workspaceID, wConfig := range workspaces {
		for _, conn := range wConfig.Connections {
			enabled, _ := misc.MapLookup(
				conn.Config, "source", "syncSettings", "errorDetailsConfig", "enabled",
			).(bool)
			k := connectionKey{sourceID: conn.SourceID, destinationID: conn.DestinationID}
			index[k] = enabled
			if !enabled {
				// A connection that captures nothing would only ever contribute
				// zero-valued series - one per connection, per pod, per push.
				continue
			}
			// Built ahead of the records so the hot path never calls NewTaggedStat.
			sk := captureStatKey{connectionKey: k, workspaceID: workspaceID}
			counters[sk] = newCaptureCounters(d.statFactory, sk)
		}
	}
	d.connectionsMu.Lock()
	d.connections = index
	d.captureStats = counters
	d.connectionsMu.Unlock()
	d.configLoadedOnce.Do(func() { close(d.configLoaded) })
}

// connectionEnabled resolves the setting for one connection. It fails closed: an
// unknown connection, a missing path or a non-boolean value all resolve to false.
func (d *syncSettingDelegate) connectionEnabled(sourceID, destinationID string) bool {
	d.connectionsMu.RLock()
	defer d.connectionsMu.RUnlock()
	return d.connections[connectionKey{sourceID: sourceID, destinationID: destinationID}]
}

// configWaitTimeout bounds the startup-window wait in awaitConfig. The caller's context
// belongs to a router status-update transaction and can be minutes long, which is far
// too long to hold that path; failing after a few seconds turns the wait into a normal
// batch error that the router retries, by which time the config has landed.
const configWaitTimeout = 30 * time.Second

// awaitConfig blocks until the first backend-config push has been indexed.
//
// Only the pin-computing path calls it, and only on a genuine cache miss. Without it a
// failed record arriving in the window between construction and the first push would
// resolve against an empty index and pin the run to false for the whole retention -
// permanently wrong, with nothing to indicate it. After the first push this is a read
// from a closed channel.
func (d *syncSettingDelegate) awaitConfig(ctx context.Context) error {
	select {
	case <-d.configLoaded:
		return nil
	default:
	}
	ctx, cancel := context.WithTimeout(ctx, configWaitTimeout)
	defer cancel()
	select {
	case <-d.configLoaded:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for the first backend config push: %w", ctx.Err())
	}
}

// staticSyncSettingDelegate answers every question the same way.
//
// The SyncSettingDelegate interface takes an unexported type on purpose, so the only
// real implementation is the one in this package; this is the stand-in for callers
// that need a delegate which decides nothing - tests, and deployments where the
// feature is switched off entirely.
type staticSyncSettingDelegate struct {
	errorResponse string
	err           error
}

// NewStaticSyncSettingDelegate returns a SyncSettingDelegate that always answers with
// errorResponse and err. Intended for tests.
func NewStaticSyncSettingDelegate(errorResponse string, err error) SyncSettingDelegate {
	return &staticSyncSettingDelegate{errorResponse: errorResponse, err: err}
}

func (s *staticSyncSettingDelegate) GetErrorResponse(_ context.Context, _ statKey, _ *jobsdb.JobStatusT) (string, error) {
	return s.errorResponse, s.err
}
