package rsources

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitutf8 "github.com/rudderlabs/rudder-go-kit/utf8"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

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

// errorCaptureSettings holds the reloadable knobs plus the logger used for the
// per-publish capture summary.
type errorCaptureSettings struct {
	log                logger.Logger
	enabled            config.ValueLoader[bool]
	maxErrorLength     config.ValueLoader[int]
	blockedConnections config.ValueLoader[[]string]
	blockedWorkspaces  config.ValueLoader[[]string]
}

func newErrorCaptureSettings(conf *config.Config, log logger.Logger) errorCaptureSettings {
	if log == nil {
		log = logger.NewLogger().Child("rsources")
	}
	return errorCaptureSettings{
		log:                log,
		enabled:            conf.GetReloadableBoolVar(false, captureErrorDetailKey),
		maxErrorLength:     conf.GetReloadableIntVar(defaultMaxErrorLength, 1, maxErrorLengthKey),
		blockedConnections: conf.GetReloadableStringSliceVar(nil, blockedConnectionsKey),
		blockedWorkspaces:  conf.GetReloadableStringSliceVar(nil, blockedWorkspacesKey),
	}
}

// errorCaptureSettingsCache memoises the settings per config instance.
var errorCaptureSettingsCache sync.Map // *config.Config -> errorCaptureSettings

// sharedErrorCaptureSettings returns the settings bound to the default config,
// registering the reloadable variables at most once per config instance.
//
// Memoised because NewStatsCollector runs once per status-update batch on the router
// and batchrouter hot paths, and config.GetReloadable*Var takes a global write lock
// on every call - registering per collector would put four lock acquisitions plus a
// logger allocation on that path.
//
// Keyed on the config instance rather than memoised once per process because
// config.Reset() swaps config.Default (the rETL integration harness does this at
// startup): settings bound to a discarded config would silently stop seeing
// config.Set, and capture failing closed with no signal is expensive to debug.
func sharedErrorCaptureSettings() errorCaptureSettings {
	conf := config.Default
	if cached, ok := errorCaptureSettingsCache.Load(conf); ok {
		return cached.(errorCaptureSettings)
	}
	// A concurrent loser here still ends up with the same *Reloadable pointers:
	// go-kit dedupes the registration by config key.
	actual, _ := errorCaptureSettingsCache.LoadOrStore(conf, newErrorCaptureSettings(conf, nil))
	return actual.(errorCaptureSettings)
}

// errorCaptureGate is a snapshot of the reloadable settings, taken once per
// CollectFailedRecords call so that every record in a batch is judged by the same
// values and a blacklist edit takes effect on the next batch without a restart.
type errorCaptureGate struct {
	enabled            bool
	maxErrorLength     int
	blockedConnections []string
	blockedWorkspaces  []string
}

func (s errorCaptureSettings) snapshot() errorCaptureGate {
	return errorCaptureGate{
		enabled:            s.enabled.Load(),
		maxErrorLength:     s.maxErrorLength.Load(),
		blockedConnections: s.blockedConnections.Load(),
		blockedWorkspaces:  s.blockedWorkspaces.Load(),
	}
}

// blocked reports whether the operator storm blacklist suppresses the message for
// this connection or workspace, along with the scope that matched.
//
// The connection key is the server-side `<sourceID>:<destinationID>` identity, not
// the control-plane connection id.
func (g errorCaptureGate) blocked(sourceID, destinationID, workspaceID string) (scope string, blocked bool) {
	if len(g.blockedConnections) > 0 &&
		slices.Contains(g.blockedConnections, sourceID+":"+destinationID) {
		return blockedScopeConnection, true
	}
	if workspaceID != "" && len(g.blockedWorkspaces) > 0 &&
		slices.Contains(g.blockedWorkspaces, workspaceID) {
		return blockedScopeWorkspace, true
	}
	return "", false
}

// errorResponseEnvelopeKeys lists, in resolution order, the keys under which the
// pipeline wraps the final recorded error on a job status' ErrorResponse:
//
//	reason   - drain/abort and retry exhaustion (router/worker.go,
//	           batchrouter/worker.go, batchrouter/handle.go); our own reason, always
//	           paired with error code 410
//	response - router HTTP delivery (router/worker.go); the destination's body,
//	           already trimmed to 10KB upstream
//	Error    - batchrouter object-storage upload and async upload
//	error    - batchrouter async poll
//
// `reason` outranks `response` because a drain enhances the PREVIOUS status'
// ErrorResponse, so a drained job carries both: the stale destination body from the
// last delivery attempt and the reason we finally gave up. Every RS abort reports
// code 410 and the raw text is the only thing that distinguishes them, so the reason
// is the "final error RudderStack recorded".
var errorResponseEnvelopeKeys = [...]string{"reason", "response", "Error", "error"}

// unwrapErrorResponse extracts the error text out of the job status' ErrorResponse
// envelope. An envelope we do not recognise, or one that is not a JSON object,
// yields an empty string rather than the raw wrapper - storing wrapper bookkeeping
// (firstAttemptedAt, dontBatch, ...) as if it were the customer's error would be
// worse than storing nothing.
func unwrapErrorResponse(errorResponse json.RawMessage) string {
	if len(errorResponse) == 0 {
		return ""
	}
	parsed := gjson.ParseBytes(errorResponse)
	if !parsed.IsObject() {
		return ""
	}
	var found [len(errorResponseEnvelopeKeys)]string
	parsed.ForEach(func(key, value gjson.Result) bool {
		// An exact switch on the key rather than gjson path lookups: gjson falls
		// back to a case-insensitive match, which would conflate `Error`/`error`.
		if i := slices.Index(errorResponseEnvelopeKeys[:], key.Str); i >= 0 {
			found[i] = value.String()
		}
		return true
	})
	for _, v := range found {
		if v != "" {
			return v
		}
	}
	return ""
}

// captureErrorText produces the value stored in `error_response` for one failed
// record: unwrap, then cap, then sanitise.
//
// The cap runs before sanitisation on purpose. SanitizeJSON no longer replaces
// invalid utf8 (only NUL), so validation has to happen here, and it has to happen on
// the already-clipped value: clipping first and validating after guarantees the
// stored bytes are valid utf8 no matter where the cut landed. Sanitisation is
// length-preserving or shrinking, so it can never push the value back over the cap.
//
// No truncation marker is added - the clip is silent by design.
func captureErrorText(errorResponse json.RawMessage, maxBytes int) (text string, clipped bool) {
	text = unwrapErrorResponse(errorResponse)
	if text == "" {
		return "", false
	}
	if maxBytes <= 0 {
		// Misconfigured cap: fail closed rather than storing unbounded text.
		return "", true
	}
	text, clipped = clipToBytes(text, maxBytes)
	return sanitizeErrorText(text), clipped
}

// clipToBytes truncates s to at most maxBytes, cutting on a rune boundary so that a
// multi-byte rune is never split in half.
func clipToBytes(s string, maxBytes int) (string, bool) {
	if len(s) <= maxBytes {
		return s, false
	}
	end := maxBytes
	// Walk back to the start of the rune straddling the cut. A rune is at most
	// utf8.UTFMax bytes, so at most UTFMax-1 steps are ever needed; bounding the
	// walk stops malformed input from eating the whole value.
	for i := 0; i < utf8.UTFMax-1 && end > 0 && !utf8.RuneStart(s[end]); i++ {
		end--
	}
	return s[:end], true
}

// sanitizeErrorText makes the captured text safe to store in a postgres text column:
// NUL bytes are stripped (postgres rejects them) and invalid utf8 byte sequences are
// replaced in place with a single-byte replacement character.
func sanitizeErrorText(s string) string {
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	if utf8.ValidString(s) {
		return s
	}
	b := []byte(s)
	kitutf8.Sanitize(b)
	return string(b)
}

// errorCaptureCounters is the aggregate capture outcome for one connection within a
// single publish. Counts and identifiers only - never the message body.
type errorCaptureCounters struct {
	workspaceID   string
	captured      int
	clipped       int
	suppressed    int
	suppressScope string
}

// WithErrorCaptureSettings overrides the process-wide error capture settings. Only
// meant for tests: production collectors share one registration of the reloadable
// config variables.
func WithErrorCaptureSettings(
	log logger.Logger,
	enabled config.ValueLoader[bool],
	maxErrorLength config.ValueLoader[int],
	blockedConnections, blockedWorkspaces config.ValueLoader[[]string],
) OptFunc {
	return func(r *statsCollector) {
		if log != nil {
			r.errorCapture.log = log
		}
		if enabled != nil {
			r.errorCapture.enabled = enabled
		}
		if maxErrorLength != nil {
			r.errorCapture.maxErrorLength = maxErrorLength
		}
		if blockedConnections != nil {
			r.errorCapture.blockedConnections = blockedConnections
		}
		if blockedWorkspaces != nil {
			r.errorCapture.blockedWorkspaces = blockedWorkspaces
		}
	}
}

// captureErrorResponse returns the final recorded error text to store for an aborted
// job status, or "" when capture is not allowed.
//
// The predicate is: the global switch, AND the connection's opt-in, AND neither the
// connection nor the workspace being on the operator storm blacklist. Suppression
// removes only the message - {Record, Code} and the retry path are untouched.
func (r *statsCollector) captureErrorResponse(gate errorCaptureGate, key statKey, jobStatus *jobsdb.JobStatusT) string {
	if !gate.enabled {
		return ""
	}
	if _, optedIn := r.captureErrorJobIds[jobStatus.JobID]; !optedIn {
		return ""
	}
	if scope, blocked := gate.blocked(key.SourceID, key.DestinationID, jobStatus.WorkspaceId); blocked {
		c := r.errorCaptureCounters(key, jobStatus.WorkspaceId)
		c.suppressed++
		c.suppressScope = scope
		return ""
	}
	text, clipped := captureErrorText(jobStatus.ErrorResponse, gate.maxErrorLength)
	c := r.errorCaptureCounters(key, jobStatus.WorkspaceId)
	if text != "" {
		c.captured++
	}
	if clipped {
		c.clipped++
	}
	return text
}

func (r *statsCollector) errorCaptureCounters(key statKey, workspaceID string) *errorCaptureCounters {
	if r.errorCaptureIndex == nil {
		r.errorCaptureIndex = make(map[statKey]*errorCaptureCounters)
	}
	c, ok := r.errorCaptureIndex[key]
	if !ok {
		c = &errorCaptureCounters{}
		r.errorCaptureIndex[key] = c
	}
	if c.workspaceID == "" {
		c.workspaceID = workspaceID
	}
	return c
}

// reportErrorCapture emits the capture outcome for one connection, once per publish.
//
// Aggregate granularity only - the collector sees tens of millions of records a day
// and per-record telemetry would be a self-inflicted storm. The message body, the
// failed row's values and anything credential-shaped are never logged or tagged: a
// captured error may echo customer PII and occasionally a secret, so telemetry
// carries counts, identifiers and the outcome only.
func (r *statsCollector) reportErrorCapture(k statKey) {
	c, ok := r.errorCaptureIndex[k]
	if !ok {
		return
	}
	tags := stats.Tags{
		"sourceId":      k.SourceID,
		"destinationId": k.DestinationID,
		"workspaceId":   c.workspaceID,
	}
	if c.captured > 0 {
		r.statFactory.NewTaggedStat(rsourcesErrorCaptured, stats.CountType, tags).Count(c.captured)
	}
	if c.clipped > 0 {
		r.statFactory.NewTaggedStat(rsourcesErrorClipped, stats.CountType, tags).Count(c.clipped)
	}
	if c.suppressed > 0 {
		r.statFactory.NewTaggedStat(rsourcesErrorSuppressed, stats.CountType, stats.Tags{
			"scope": c.suppressScope,
		}).Count(c.suppressed)
		r.errorCapture.log.Warnn("rsources: error capture blocked",
			obskit.SourceID(k.SourceID),
			obskit.DestinationID(k.DestinationID),
			obskit.WorkspaceID(c.workspaceID),
			logger.NewStringField("job_run_id", k.jobRunId),
			logger.NewStringField("reason", c.suppressScope),
		)
	}
	r.errorCapture.log.Infon("rsources: error capture summary",
		obskit.SourceID(k.SourceID),
		obskit.DestinationID(k.DestinationID),
		obskit.WorkspaceID(c.workspaceID),
		logger.NewStringField("job_run_id", k.jobRunId),
		logger.NewIntField("capturedCount", int64(c.captured)),
		logger.NewIntField("clippedCount", int64(c.clipped)),
		logger.NewIntField("suppressedByBlacklist", int64(c.suppressed)),
	)
}
