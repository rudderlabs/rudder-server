package processor

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// The rETL "capture the final recorded error" opt-in is a per-connection setting the control plane
// stores on the connection itself, at config.source.syncSettings.errorDetailsConfig.enabled. The
// processor resolves it from the backend-config subscription it already keeps
// (config.connectionConfigMap, rebuilt on every config push) and stamps the answer on the job's
// ParametersT, which is the only input the failed-records collector parses.
//
// Nothing client-supplied feeds this decision: no request header, no payload field. A forged signal
// on either is inert because no code reads one.
const (
	// captureOptInPinTTL bounds how long a job run's decision stays pinned. rETL syncs are far
	// shorter than this; the TTL only has to outlive the longest run so a mid-run config push
	// cannot split one sync's records between two decisions.
	captureOptInPinTTL = 24 * time.Hour

	// captureOptInMaxPins caps the pin store. The key space cannot be assumed bounded: a job run id
	// reaches the processor from the job's parameters, and the gateway seeds those from the first
	// event's context.sources.job_run_id when present (gateway/handle.go), so a client can mint
	// arbitrarily many. At the cap we stop pinning new runs instead of growing: resolution still
	// happens per record and is still fail-closed, only the whole-run uniformity guarantee lapses
	// for runs that arrive while the store is full.
	captureOptInMaxPins = 50_000

	// captureOptInSweepInterval throttles the O(n) sweep that reclaims pins nobody looked up again
	// (a run with a single failed record leaves one behind). Expired pins are otherwise reclaimed
	// lazily, when the run they belong to is resolved again.
	captureOptInSweepInterval = time.Minute
)

// captureOptInPin is one pinned decision. storedAt is the first resolution's timestamp and is never
// refreshed by later reads: a pin must age out on a fixed schedule rather than live as long as its
// run keeps producing records.
type captureOptInPin struct {
	value    bool
	storedAt time.Time
}

// captureOptInPins pins the resolved opt-in per job run id, so that every record of one sync carries
// the same decision even if the connection's config changes mid-run. A toggle then takes effect from
// the next sync, which is the contract the settings copy states.
type captureOptInPins struct {
	mu   sync.RWMutex
	pins map[string]captureOptInPin

	ttl           time.Duration
	maxPins       int
	sweepInterval time.Duration
	now           func() time.Time

	lastSweep time.Time
}

func newCaptureOptInPins() *captureOptInPins {
	return &captureOptInPins{
		pins:          make(map[string]captureOptInPin),
		ttl:           captureOptInPinTTL,
		maxPins:       captureOptInMaxPins,
		sweepInterval: captureOptInSweepInterval,
		now:           time.Now,
	}
}

// lookup returns the live pinned decision for jobRunID, if there is one. An expired pin reports a
// miss and is left in place for pin() to overwrite or for the sweep to reclaim, so that the read
// path never needs the write lock.
func (p *captureOptInPins) lookup(jobRunID string) (value, ok bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	pin, found := p.pins[jobRunID]
	if !found || p.now().Sub(pin.storedAt) >= p.ttl {
		return false, false
	}
	return pin.value, true
}

// pin stores value for jobRunID and returns it, unless another goroutine pinned the run first while
// value was being resolved - then the stored decision wins, because the run must stay uniform.
func (p *captureOptInPins) pin(jobRunID string, value bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.now()
	if pin, found := p.pins[jobRunID]; found && now.Sub(pin.storedAt) < p.ttl {
		return pin.value
	}
	if now.Sub(p.lastSweep) >= p.sweepInterval {
		p.sweep(now)
	}
	if _, replacing := p.pins[jobRunID]; !replacing && len(p.pins) >= p.maxPins {
		return value
	}
	p.pins[jobRunID] = captureOptInPin{value: value, storedAt: now}
	return value
}

// sweep drops every expired pin. The caller must hold the write lock.
func (p *captureOptInPins) sweep(now time.Time) {
	for jobRunID, pin := range p.pins {
		if now.Sub(pin.storedAt) >= p.ttl {
			delete(p.pins, jobRunID)
		}
	}
	p.lastSweep = now
}

// size reports how many pins are held, expired ones included.
func (p *captureOptInPins) size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.pins)
}

// captureErrorForRun answers, for a single event, whether the failed-records collector should keep
// the final recorded error text. Non-rETL traffic (no job run id) is never captured and never
// pinned; every rETL record of a run gets the run's first answer.
func (proc *Handle) captureErrorForRun(sourceID, destinationID, jobRunID string) bool {
	if jobRunID == "" {
		return false
	}
	if pinned, ok := proc.captureOptInPins.lookup(jobRunID); ok {
		return pinned
	}
	return proc.captureOptInPins.pin(jobRunID, proc.resolveCaptureErrorOptIn(sourceID, destinationID))
}

// resolveCaptureErrorOptIn reads the opt-in off the connection's backend config. It fails closed: an
// unknown connection, a missing path, or a non-boolean value all resolve to false.
func (proc *Handle) resolveCaptureErrorOptIn(sourceID, destinationID string) bool {
	conn := proc.getConnectionConfig(connection{sourceID: sourceID, destinationID: destinationID})
	enabled, _ := misc.MapLookup(conn.Config, "source", "syncSettings", "errorDetailsConfig", "enabled").(bool)
	return enabled
}
