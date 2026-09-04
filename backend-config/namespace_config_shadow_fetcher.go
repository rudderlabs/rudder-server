package backendconfig

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

// shadowConfigFetcher serves the primary fetcher's configs and, at most once per interval, fetches
// the candidate's on the side and compares the two (§3.3 in design doc). The primary stays
// authoritative: nothing the candidate does - an error, a panic, a divergence - reaches the
// returned config, and the candidate is only fetched after the primary has returned, off the poll
// goroutine, so the poll's latency is untouched.
type shadowConfigFetcher struct {
	primary   configFetcher
	candidate shadowCandidate
	logger    logger.Logger
	comparer  *shadowComparer

	interval time.Duration
	lastRun  time.Time // touched only by the poll goroutine
	// at most one comparison runs at a time: a poll that finds one still running skips its turn.
	// The candidate keeps incremental state across samples, and this flag is also what orders
	// that state between one sample goroutine and the next
	sampling atomic.Bool
}

// shadowCandidate is the fetcher under observation. Beyond fetching, the sample gate needs to know
// how fresh its definition catalogues are: a definition edit moves no workspace timestamp.
type shadowCandidate interface {
	configFetcher
	definitionsUpdatedAt() time.Time
}

func newShadowConfigFetcher(nc *namespaceConfig) (*shadowConfigFetcher, error) {
	candidate, err := newV2ConfigFetcher(nc)
	if err != nil {
		return nil, err
	}
	log := nc.logger.Withn(logger.NewStringField("component", "bcv2-shadow"))
	uploader, err := newShadowSamplingUploader(nc.config, log)
	if err != nil {
		// degrade to counters and logs rather than blocking startup: only the artifacts are lost
		uploader = nil
		log.Errorn("shadow sampling uploader could not be created", obskit.Error(err))
	}
	return &shadowConfigFetcher{
		primary:   newV1ConfigFetcher(nc),
		candidate: candidate,
		logger:    log,
		interval:  nc.config.GetDurationVar(5, time.Minute, "BackendConfigShadow.samplingInterval"),
		comparer:  newShadowComparer(nc.config, log, nc.stats, nc.namespace, uploader),
	}, nil
}

// Get returns the primary's configs, and may leave a comparison of the two versions running
// behind itself.
func (f *shadowConfigFetcher) Get(ctx context.Context) (map[string]ConfigT, error) {
	configs, err := f.primary.Get(ctx)
	if err == nil {
		f.sample(ctx, configs)
	}
	return configs, err
}

func (f *shadowConfigFetcher) sample(ctx context.Context, primary map[string]ConfigT) {
	if time.Since(f.lastRun) < f.interval {
		return
	}
	if !f.sampling.CompareAndSwap(false, true) {
		f.comparer.turnSkipped.Increment() // an overrunning sample retries at the next poll
		return
	}
	f.lastRun = time.Now()
	// handing the primary's map to the goroutine is safe: every poll builds a fresh map and never
	// mutates a previous one
	go func() {
		defer f.sampling.Store(false)
		defer func() {
			// the transform is young code, and this goroutine exists on the premise that the
			// candidate cannot hurt the data plane
			if r := recover(); r != nil {
				f.comparer.errored("panic")
				f.logger.Errorn("shadow comparison panicked",
					logger.NewStringField("panic", fmt.Sprint(r)),
					logger.NewStringField("stack", string(debug.Stack())),
				)
			}
		}()
		f.runSample(ctx, primary)
	}()
}

func (f *shadowConfigFetcher) runSample(ctx context.Context, primary map[string]ConfigT) {
	candidate, err := f.candidate.Get(ctx)
	if err != nil {
		f.comparer.errored("fetch") // the candidate has already logged the error itself
		return
	}
	defer f.comparer.comparisonTime.RecordDuration()()
	// anything written between the two fetches carries a timestamp later than everything the
	// primary observed, making the sample incomparable (§3.3.2 in design doc). The candidate's
	// side folds in the definition catalogues, which no workspace timestamp covers
	candidateMax := newestUpdatedAt(candidate)
	if definitions := f.candidate.definitionsUpdatedAt(); definitions.After(candidateMax) {
		candidateMax = definitions
	}
	if candidateMax.After(newestUpdatedAt(primary)) {
		f.comparer.sampleDropped.Increment()
		return
	}
	f.comparer.compare(ctx, primary, candidate)
}

func newestUpdatedAt(configs map[string]ConfigT) time.Time {
	var newest time.Time
	for _, config := range configs {
		if config.UpdatedAt.After(newest) {
			newest = config.UpdatedAt
		}
	}
	return newest
}
