package multitenant_test

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
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

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

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
