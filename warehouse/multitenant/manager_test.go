package multitenant_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ backendconfig.BackendConfig = (*mockBackendConfig)(nil)

type mockBackendConfig struct {
	config map[string]backendconfig.ConfigT
}

func (*mockBackendConfig) WaitForConfig(_ context.Context)          {}
func (*mockBackendConfig) Stop()                                    {}
func (*mockBackendConfig) StartWithIDs(_ context.Context, _ string) {}
func (*mockBackendConfig) SetUp() error                             { return nil }
func (*mockBackendConfig) AccessToken() string                      { return "" }
func (*mockBackendConfig) Identity() identity.Identifier            { return nil }

func (m *mockBackendConfig) Get(context.Context) (map[string]backendconfig.ConfigT, error) {
	return m.config, nil
}

func (m *mockBackendConfig) Subscribe(ctx context.Context, _ backendconfig.Topic) pubsub.DataChannel {
	ch := make(chan pubsub.DataEvent)
	go func() {
		ch <- pubsub.DataEvent{
			Data: m.config,
		}

		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func TestDegradeWorkspace(t *testing.T) {
	testcase := []struct {
		name                string
		namespaceWorkspaces []string
		degradedWorkspaces  []string
		expectedWorkspaces  []string
	}{
		{
			name:                "no degraded workspaces",
			namespaceWorkspaces: []string{"workspace1", "workspace2"},
			degradedWorkspaces:  []string{},
			expectedWorkspaces:  []string{"workspace1", "workspace2"},
		},
		{
			name:                "one degraded workspace",
			namespaceWorkspaces: []string{"workspace1", "workspace2"},
			degradedWorkspaces:  []string{"workspace1"},
			expectedWorkspaces:  []string{"workspace2"},
		},
		{
			name:                "multiple degraded workspaces",
			namespaceWorkspaces: []string{"workspace1", "workspace2", "workspace3"},
			degradedWorkspaces:  []string{"workspace1", "workspace3"},
			expectedWorkspaces:  []string{"workspace2"},
		},
		{
			name:                "all workspaces degraded",
			namespaceWorkspaces: []string{"workspace1", "workspace2"},
			degradedWorkspaces:  []string{"workspace1", "workspace2"},
			expectedWorkspaces:  []string{},
		},
	}

	for _, tc := range testcase {
		t.Run(tc.name, func(t *testing.T) {
			backendConfig := make(map[string]backendconfig.ConfigT)
			expectedConfig := make(map[string]backendconfig.ConfigT)

			for _, workspace := range tc.namespaceWorkspaces {
				backendConfig[workspace] = backendconfig.ConfigT{
					WorkspaceID: workspace,
				}
			}

			for _, workspace := range tc.expectedWorkspaces {
				expectedConfig[workspace] = backendconfig.ConfigT{
					WorkspaceID: workspace,
				}
			}

			c := config.New()
			c.Set("Warehouse.degradedWorkspaceIDs", tc.degradedWorkspaces)

			m := multitenant.New(c, &mockBackendConfig{
				config: backendConfig,
			})

			ctx := t.Context()

			ch := m.WatchConfig(ctx)
			config := <-ch

			require.Equal(t, expectedConfig, config, "filter configuration by degraded workspaces")

			require.Equal(t, tc.degradedWorkspaces, m.DegradedWorkspaces())

			for _, workspace := range tc.degradedWorkspaces {
				require.True(t, m.DegradedWorkspace(workspace))
			}
		})
	}
}

func TestDegradedWorkspaceReload(t *testing.T) {
	c := config.New()
	m := multitenant.New(c, &mockBackendConfig{})
	require.False(t, m.DegradedWorkspace("workspace1"))

	c.Set("Warehouse.degradedWorkspaceIDs", []string{"workspace1"})
	require.True(t, m.DegradedWorkspace("workspace1"))
	require.Equal(t, []string{"workspace1"}, m.DegradedWorkspaces())

	list := m.DegradedWorkspaces()
	list[0] = "other"
	list = append(list, "workspace2")
	require.Equal(t, []string{"other", "workspace2"}, list)
	require.Equal(t, []string{"workspace1"}, m.DegradedWorkspaces())

	c.Set("Warehouse.degradedWorkspaceIDs", []string{"workspace2"})
	require.False(t, m.DegradedWorkspace("workspace1"))
	require.True(t, m.DegradedWorkspace("workspace2"))
}

func TestWatchConfigReload(t *testing.T) {
	full := map[string]backendconfig.ConfigT{
		"workspace1": {WorkspaceID: "workspace1"},
		"workspace2": {WorkspaceID: "workspace2"},
	}
	c := config.New()
	m := multitenant.New(c, &mockBackendConfig{config: full})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	first, second := m.WatchConfig(ctx), m.WatchConfig(ctx)
	for _, ch := range []<-chan map[string]backendconfig.ConfigT{first, second} {
		require.Equal(t, full, receiveConfig(t, ch))
	}

	c.Set("Warehouse.degradedWorkspaceIDs", []string{"workspace1"})
	filtered := map[string]backendconfig.ConfigT{"workspace2": full["workspace2"]}
	for _, ch := range []<-chan map[string]backendconfig.ConfigT{first, second} {
		require.Equal(t, filtered, receiveConfig(t, ch))
	}

	c.Set("Warehouse.degradedWorkspaceIDs", []string{})
	for _, ch := range []<-chan map[string]backendconfig.ConfigT{first, second} {
		require.Equal(t, full, receiveConfig(t, ch))
	}

	cancel()
	for _, ch := range []<-chan map[string]backendconfig.ConfigT{first, second} {
		_, ok := <-ch
		require.False(t, ok)
	}
	// Reloading after subscribers exit cannot block the synchronous Set callback.
	c.Set("Warehouse.degradedWorkspaceIDs", []string{"workspace2"})
}

func TestWatchConfigCanceledSubscriber(t *testing.T) {
	full := map[string]backendconfig.ConfigT{"workspace1": {WorkspaceID: "workspace1"}}
	c := config.New()
	m := multitenant.New(c, &mockBackendConfig{config: full})
	ctx, cancel := context.WithCancel(t.Context())
	closed := m.WatchConfig(ctx)
	require.Equal(t, full, receiveConfig(t, closed))
	active := m.WatchConfig(t.Context())
	require.Equal(t, full, receiveConfig(t, active))

	cancel()
	_, ok := <-closed
	require.False(t, ok)
	c.Set("Warehouse.degradedWorkspaceIDs", []string{"workspace1"})
	require.Empty(t, receiveConfig(t, active))
}

func receiveConfig(t *testing.T, ch <-chan map[string]backendconfig.ConfigT) map[string]backendconfig.ConfigT {
	t.Helper()
	select {
	case cfg, ok := <-ch:
		require.True(t, ok)
		return cfg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for filtered config")
		return nil
	}
}

func TestHasConnection(t *testing.T) {
	const workspace, source, destination = "workspace", "source", "destination"
	full := map[string]backendconfig.ConfigT{
		workspace: {WorkspaceID: workspace, Sources: []backendconfig.SourceT{{
			ID: source, Destinations: []backendconfig.DestinationT{{
				ID: destination, DestinationDefinition: backendconfig.DestinationDefinitionT{Name: warehouseutils.RS},
			}, {
				ID: "unsupported", DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "UNSUPPORTED"},
			}},
		}}},
	}
	c := config.New()
	m := multitenant.New(c, &mockBackendConfig{config: full})
	// A delayed Run subscription must not make an already-published WatchConfig
	// snapshot unavailable to an allocator.
	exists, known := m.HasConnection(workspace, source, destination, warehouseutils.RS)
	require.False(t, known)
	require.False(t, exists)
	ch := m.WatchConfig(t.Context())
	require.Equal(t, full, receiveConfig(t, ch))

	for _, tc := range []struct {
		name, workspace, source, destination, destType string
		exists                                         bool
	}{
		{"exact connection", workspace, source, destination, warehouseutils.RS, true},
		{"legacy upload", "", source, destination, warehouseutils.RS, true},
		{"other workspace", "other", source, destination, warehouseutils.RS, false},
		{"absent destination", workspace, source, "removed", warehouseutils.RS, false},
		{"other router", workspace, source, destination, warehouseutils.BQ, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exists, known := m.HasConnection(tc.workspace, tc.source, tc.destination, tc.destType)
			require.True(t, known)
			require.Equal(t, tc.exists, exists)
		})
	}

	exists, known = m.HasConnection("", source, "unsupported", "UNSUPPORTED")
	require.True(t, known)
	require.False(t, exists)
}

func TestSourceToWorkspace(t *testing.T) {
	mapping := map[string]string{
		"source1": "workspaceA",
		"source2": "workspaceA",

		"source3": "workspaceB",
		"source4": "workspaceC",
	}

	backendConfig := make(map[string]backendconfig.ConfigT)
	for source, workspace := range mapping {
		entry, ok := backendConfig[workspace]
		if !ok {
			entry = backendconfig.ConfigT{
				WorkspaceID: workspace,
			}
		}

		if _, ok := backendConfig[workspace]; !ok {
			backendConfig[workspace] = backendconfig.ConfigT{
				WorkspaceID: workspace,
			}
		}

		entry.Sources = append(entry.Sources, backendconfig.SourceT{
			ID: source,
		})

		backendConfig[workspace] = entry
	}

	m := multitenant.New(config.New(), &mockBackendConfig{
		config: backendConfig,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var g errgroup.Group
	g.Go(func() error {
		m.Run(ctx)
		return nil
	})

	for source, workspace := range mapping {
		wID, err := m.SourceToWorkspace(ctx, source)
		require.NoError(t, err)
		require.Equal(t, workspace, wID)
	}

	wID, err := m.SourceToWorkspace(ctx, "not-found-source-id")
	require.EqualError(t, err, "sourceID: not-found-source-id not found")
	require.Equal(t, "", wID)

	cancel()
	require.NoError(t, g.Wait())

	t.Run("context canceled", func(t *testing.T) {
		m := multitenant.New(config.New(), &mockBackendConfig{
			config: backendConfig,
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		wID, err := m.SourceToWorkspace(ctx, "source1")
		require.EqualError(t, err, "context canceled")
		require.Equal(t, "", wID)
	})
}
