package cpservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func TestConnectorRun(t *testing.T) {
	t.Run("applies the flag then closes the connection on shutdown", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		c, rec, ch := newRunConnector()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()

		ch <- configEvent(wsConfWithServiceFlag("cp-router:1234", true))

		require.Eventually(t, func() bool {
			calls := rec.snapshot()
			return len(calls) == 1 && calls[0] == applyCall{url: "cp-router:1234", active: true}
		}, time.Second, 10*time.Millisecond)

		cancel()
		require.NoError(t, <-done)

		// stop() must tear the connection down on the last URL it applied.
		calls := rec.snapshot()
		require.Equal(t, applyCall{url: "cp-router:1234", active: false}, calls[len(calls)-1])
	})

	t.Run("ignores events whose Data is not the workspace config map", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		c, rec, ch := newRunConnector()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()

		// A malformed event (failing the type assertion) followed by a valid one.
		// Run consumes the channel serially, so once the valid event is applied we
		// know the malformed one was skipped rather than mis-applied.
		ch <- pubsub.DataEvent{Topic: string(backendconfig.TopicBackendConfig), Data: "not-a-config-map"}
		ch <- configEvent(wsConfWithServiceFlag("cp-router:1234", true))

		require.Eventually(t, func() bool {
			calls := rec.snapshot()
			return len(calls) == 1 && calls[0].url == "cp-router:1234"
		}, time.Second, 10*time.Millisecond)

		cancel()
		require.NoError(t, <-done)
	})

	t.Run("ignores workspaces missing the service flag", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		c, rec, ch := newRunConnector()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()

		// Only the warehouse flag present → apply must not fire for this event.
		// A trailing event that does carry the flag proves the first was skipped.
		ch <- configEvent(map[string]backendconfig.ConfigT{
			"ws-1": {ConnectionFlags: backendconfig.ConnectionFlags{
				URL:      "cp-router:0000",
				Services: map[string]bool{"warehouse": true},
			}},
		})
		ch <- configEvent(wsConfWithServiceFlag("cp-router:1234", true))

		require.Eventually(t, func() bool {
			calls := rec.snapshot()
			return len(calls) == 1 && calls[0].url == "cp-router:1234"
		}, time.Second, 10*time.Millisecond)

		cancel()
		require.NoError(t, <-done)
	})

	t.Run("returns without closing when the subscribe channel closes and nothing was applied", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		c, rec, ch := newRunConnector()
		done := make(chan error, 1)
		go func() { done <- c.Run(context.Background()) }()

		close(ch)
		require.NoError(t, <-done)
		require.Empty(t, rec.snapshot())
	})
}

// applyCall records one (url, active) invocation of the connection manager.
type applyCall struct {
	url    string
	active bool
}

// recordingConnManager stands in for controlplane.ConnectionManager.Apply so the
// Run loop's decisions are observable without dialing a real cp-router.
type recordingConnManager struct {
	mu    sync.Mutex
	calls []applyCall
}

func (r *recordingConnManager) apply(url string, active bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, applyCall{url: url, active: active})
}

func (r *recordingConnManager) snapshot() []applyCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]applyCall(nil), r.calls...)
}

// fakeBackendConfig implements just enough of backendconfig.BackendConfig to feed
// the Run loop a channel of events. Every other method is nil-embedded and will
// panic if Run ever calls it — which it must not.
type fakeBackendConfig struct {
	backendconfig.BackendConfig
	ch chan pubsub.DataEvent
}

func (f *fakeBackendConfig) Subscribe(context.Context, backendconfig.Topic) pubsub.DataChannel {
	return f.ch
}

func newRunConnector() (*Connector, *recordingConnManager, chan pubsub.DataEvent) {
	rec := &recordingConnManager{}
	ch := make(chan pubsub.DataEvent, 8)
	c := &Connector{
		log:           logger.NOP,
		backendConfig: &fakeBackendConfig{ch: ch},
		applyConn:     rec.apply,
	}
	return c, rec, ch
}

// configEvent wraps a workspace config map in the pubsub envelope Run expects.
func configEvent(configs map[string]backendconfig.ConfigT) pubsub.DataEvent {
	return pubsub.DataEvent{Topic: string(backendconfig.TopicBackendConfig), Data: configs}
}

// wsConfWithServiceFlag builds a single-workspace config carrying the cp-router flag.
func wsConfWithServiceFlag(url string, active bool) map[string]backendconfig.ConfigT {
	return map[string]backendconfig.ConfigT{
		"ws-1": {ConnectionFlags: backendconfig.ConnectionFlags{
			URL:      url,
			Services: map[string]bool{ServiceName: active},
		}},
	}
}
