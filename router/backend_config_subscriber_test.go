package router

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

// configEventWithDestinationDef builds a TopicBackendConfig event carrying a
// single GA destination whose definition config holds the given keys.
func configEventWithDestinationDef(defConfig map[string]any) pubsub.DataEvent {
	def := backendconfig.DestinationDefinitionT{
		ID:          gaDestinationDefinitionID,
		Name:        "GA",
		DisplayName: "Google Analytics",
		Config:      defConfig,
	}
	return pubsub.DataEvent{
		Topic: string(backendconfig.TopicBackendConfig),
		Data: map[string]backendconfig.ConfigT{
			workspaceID: {
				WorkspaceID: workspaceID,
				Sources: []backendconfig.SourceT{
					{
						WorkspaceID: workspaceID,
						ID:          sourceIDEnabled,
						WriteKey:    writeKeyEnabled,
						Enabled:     true,
						Destinations: []backendconfig.DestinationT{
							{
								ID:                    gaDestinationID,
								Name:                  "ga dest",
								DestinationDefinition: def,
								Enabled:               true,
								IsProcessorEnabled:    true,
							},
						},
					},
				},
			},
		},
	}
}

// newSubscriberTestHandle wires a Handle with just enough state for
// backendConfigSubscriber: the errgroup pair mirrors what Setup installs in
// backgroundCtx/backgroundGroup, and the mock subscribes the way the real
// pubsub does, closing the channel once the subscription context is done.
func newSubscriberTestHandle(t *testing.T, events ...pubsub.DataEvent) (*Handle, context.CancelFunc, *errgroup.Group) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)

	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, len(events))
			for _, evt := range events {
				ch <- evt
			}
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		})

	rt := &Handle{
		destType:                 "GA",
		backendConfig:            mockBackendConfig,
		backgroundCtx:            gctx,
		backgroundGroup:          g,
		backendConfigInitialized: make(chan bool, 1),
	}
	return rt, cancel, g
}

// Regression for the router shutdown leak: the subscriber used to subscribe
// with context.TODO() and run outside backgroundGroup, so Shutdown could not
// reach it and two goroutines leaked per router instance.
func TestBackendConfigSubscriberExitsOnShutdown(t *testing.T) {
	rt, cancel, g := newSubscriberTestHandle(t, configEventWithDestinationDef(nil))

	g.Go(crash.Wrapper(func() error {
		rt.backendConfigSubscriber()
		return nil
	}))

	select {
	case <-rt.backendConfigInitialized:
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber never processed the initial config event")
	}

	cancel()

	waitDone := make(chan error, 1)
	go func() { waitDone <- g.Wait() }()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("backendConfigSubscriber is still running after shutdown; goroutine leaked")
	}
}

// The subscription must be tied to the router lifecycle: a context whose Done
// channel never fires (context.TODO()) cannot be cancelled by Shutdown.
func TestBackendConfigSubscriberUsesRouterContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, gctx := errgroup.WithContext(ctx)
	defer func() { _ = g.Wait() }()

	subscribedCtx := make(chan context.Context, 1)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			subscribedCtx <- ctx
			ch := make(chan pubsub.DataEvent)
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		})

	rt := &Handle{
		destType:                 "GA",
		backendConfig:            mockBackendConfig,
		backgroundCtx:            gctx,
		backgroundGroup:          g,
		backendConfigInitialized: make(chan bool, 1),
	}

	g.Go(crash.Wrapper(func() error {
		rt.backendConfigSubscriber()
		return nil
	}))

	select {
	case sc := <-subscribedCtx:
		require.NotNil(t, sc.Done(), "Subscribe was called with a non-cancellable context")
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe was never called")
	}
	cancel()
}

// Config values carried by backend-config events must land on the handle,
// through the same write path the data race on saveDestinationResponse lived
// on before it became an atomic.
func TestBackendConfigSubscriberAppliesDestinationDefinitionConfig(t *testing.T) {
	rt, cancel, g := newSubscriberTestHandle(t, configEventWithDestinationDef(map[string]any{
		"saveDestinationResponse":       true,
		"supportsDeliveredWithWarnings": true,
	}))

	g.Go(crash.Wrapper(func() error {
		rt.backendConfigSubscriber()
		return nil
	}))

	select {
	case <-rt.backendConfigInitialized:
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber never processed the initial config event")
	}

	require.True(t, rt.saveDestinationResponse.Load())
	require.True(t, rt.supportsDeliveredWithWarnings.Load())

	cancel()
	require.NoError(t, g.Wait())
}

// Exercise the write path (backendConfigSubscriber applying config events)
// against the read path used by workers on every delivery decision. With a
// plain bool this trips the race detector; the atomic keeps it clean.
func TestSaveDestinationResponseConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race exercise in short mode")
	}

	events := make([]pubsub.DataEvent, 0, 64)
	for i := 0; i < 64; i++ {
		events = append(events, configEventWithDestinationDef(map[string]any{
			"saveDestinationResponse": i%2 == 0,
		}))
	}
	rt, cancel, g := newSubscriberTestHandle(t, events...)
	rt.backendConfigInitialized = make(chan bool, len(events))

	g.Go(crash.Wrapper(func() error {
		rt.backendConfigSubscriber()
		return nil
	}))

	readersDone := make(chan struct{})
	go func() {
		defer close(readersDone)
		for i := 0; i < 10000; i++ {
			_ = !rt.saveDestinationResponse.Load()
		}
	}()

	<-readersDone
	cancel()
	require.NoError(t, g.Wait())
}
