// Package alpha provides a hackathon-only fire-and-forget HTTP dispatcher
// that forwards a small subset of post-user-transformation event metadata to
// an external "alpha" service.
//
// This is intentionally minimal — no metrics, no tests, no graceful drain.
// It is wired into the processor at the post-UT pipeline stage and is meant
// to be deleted (or hardened) after the hackathon.
package alpha

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	// channelBuffer caps how many batches can be queued for delivery before
	// new batches get dropped. Sized for trickle-volume hackathon demos
	// (1-10 events/sec → ~1-2 batches/sec).
	channelBuffer = 200
	// workerCount is how many goroutines drain the channel and POST batches
	// in parallel. With maxAttempts*retryDelay worst-case latency, each
	// worker can be tied up for several minutes during a full alpha outage.
	workerCount = 4

	// Retry policy: 10 attempts, 30s between attempts, ~5 min worst case
	// for a fast-failing alpha and ~14.5 min worst case for a hanging alpha.
	maxAttempts = 10
	retryDelay  = 30 * time.Second

	// Per-request timeout. Bumped to 60s to accommodate slower alpha
	// responses while keeping connection-refused failure mode quick.
	requestTimeout = 60 * time.Second

	// Rate-limit channel-full drop logs: log every Nth drop to avoid spam
	// during sustained outages.
	dropLogEveryN = 100
)

// IngestEvent is the per-event subset of a batch sent to the alpha service.
// JSON field names use capital "ID" to match the alpha service's Go struct.
type IngestEvent struct {
	UserID    string `json:"userID"`
	MessageID string `json:"messageID"`
	EventName string `json:"eventName"`
}

// EventsRequest is the batch payload POSTed to the alpha service. All events
// in a single request share one WorkspaceID.
type EventsRequest struct {
	WorkspaceID string        `json:"workspaceID"`
	Events      []IngestEvent `json:"events"`
}

// Dispatcher fires batches asynchronously to the alpha service with retries.
// When constructed with an empty URL, it is disabled and Dispatch is a no-op.
type Dispatcher struct {
	url       string
	enabled   bool
	logger    logger.Logger
	ch        chan EventsRequest
	client    *http.Client
	dropCount uint64
}

// NewDispatcher constructs a Dispatcher. When url is empty the dispatcher is
// disabled; Dispatch becomes a no-op and Run returns immediately. A warning
// is logged once at construction in that case.
func NewDispatcher(url string, log logger.Logger) *Dispatcher {
	d := &Dispatcher{
		url:     url,
		enabled: url != "",
		logger:  log.Child("alpha-dispatcher"),
		client:  &http.Client{Timeout: requestTimeout},
	}
	if !d.enabled {
		d.logger.Warnn("alphaServiceUrl is empty; alpha dispatcher disabled")
		return d
	}
	d.ch = make(chan EventsRequest, channelBuffer)
	d.logger.Infon("alpha dispatcher initialized",
		logger.NewStringField("url", url),
		logger.NewIntField("workers", workerCount),
		logger.NewIntField("buffer", channelBuffer),
	)
	return d
}

// Dispatch enqueues the batch for delivery. Non-blocking: if the channel is
// full, the batch is dropped and a rate-limited warning is logged. Safe to
// call when d is nil or disabled, or when the batch is empty (both no-op).
func (d *Dispatcher) Dispatch(req EventsRequest) {
	if d == nil || !d.enabled {
		return
	}
	if len(req.Events) == 0 {
		return
	}
	select {
	case d.ch <- req:
	default:
		n := atomic.AddUint64(&d.dropCount, 1)
		if n%dropLogEveryN == 1 {
			d.logger.Warnn("alpha dispatcher channel full; batch dropped",
				logger.NewStringField("workspaceId", req.WorkspaceID),
				logger.NewIntField("eventCount", int64(len(req.Events))),
				logger.NewIntField("totalDrops", int64(n)),
			)
		}
	}
}

// Run launches the worker pool and blocks until ctx is cancelled and all
// workers have exited. Intended to be called from a long-lived goroutine
// owned by the caller's lifecycle (e.g. processor.Setup's errgroup).
func (d *Dispatcher) Run(ctx context.Context) {
	if d == nil || !d.enabled {
		return
	}
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			d.worker(ctx)
		}()
	}
	wg.Wait()
}

func (d *Dispatcher) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-d.ch:
			d.postWithRetry(ctx, req)
		}
	}
}

func (d *Dispatcher) postWithRetry(ctx context.Context, req EventsRequest) {
	body, err := json.Marshal(req)
	if err != nil {
		d.logger.Warnn("alpha dispatcher: marshal failed",
			logger.NewStringField("workspaceId", req.WorkspaceID),
			logger.NewIntField("eventCount", int64(len(req.Events))),
			logger.NewStringField("error", err.Error()),
		)
		return
	}
	var lastStatus int
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		status, err := d.postOnce(ctx, body)
		if err == nil && status == http.StatusOK {
			return
		}
		lastStatus, lastErr = status, err
		if attempt == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryDelay):
		}
	}
	d.logger.Warnn("alpha dispatcher: giving up after max attempts",
		logger.NewStringField("workspaceId", req.WorkspaceID),
		logger.NewIntField("eventCount", int64(len(req.Events))),
		logger.NewIntField("attempts", int64(maxAttempts)),
		logger.NewIntField("lastStatus", int64(lastStatus)),
		logger.NewStringField("lastError", errString(lastErr)),
	)
}

func (d *Dispatcher) postOnce(ctx context.Context, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("non-200 status: %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

func errString(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}
