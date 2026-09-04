package router

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"

	mock_features "github.com/rudderlabs/rudder-server/mocks/services/transformer"
)

func newProxyTestHandle(t *testing.T, c *config.Config, declared ...bool) *Handle {
	t.Helper()
	features := mock_features.NewMockFeaturesService(gomock.NewController(t))
	for _, d := range declared {
		features.EXPECT().TransformerProxy("TEST_DEST").Return(d)
	}
	return &Handle{
		destType:                   "TEST_DEST",
		transformerFeaturesService: features,
		reloadableConfig: &reloadableConfig{
			transformerProxy: c.GetReloadableBoolVar(false, getRouterConfigKeys("transformerProxy", "TEST_DEST")...),
		},
	}
}

// transformerProxyEnabled resolves two independent inputs: what the transformer image declares on
// /features, and what Router.<DEST>.transformerProxy says. The transformer's declaration is
// authoritative — a declared destination is proxied without the env being consulted — and the env
// remains the enablement path for everything the transformer has not declared.
func TestTransformerProxyEnabled(t *testing.T) {
	testCases := []struct {
		name string
		// declared is whether the transformer's /features capability map lists this destination.
		declared bool
		// env is Router.<DEST>.transformerProxy, empty when the key is unset.
		env      string
		expected bool
	}{
		{
			name:     "declared with no env is proxied - the target state once env entries are removed",
			declared: true,
			expected: true,
		},
		{
			name:     "declared wins over an env that says false",
			declared: true,
			env:      "false",
			expected: true,
		},
		{
			name:     "declared and env agreeing is proxied - the state during migration",
			declared: true,
			env:      "true",
			expected: true,
		},
		{
			name:     "undeclared falls back to env - every destination still rolling out",
			env:      "true",
			expected: true,
		},
		{
			name:     "undeclared and env false is not proxied",
			env:      "false",
			expected: false,
		},
		{
			name:     "neither is not proxied",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			if tc.env != "" {
				c.Set("Router.TEST_DEST.transformerProxy", tc.env)
			}

			require.Equal(t, tc.expected, newProxyTestHandle(t, c, tc.declared).transformerProxyEnabled())
		})
	}
}

// Upgrading the transformer to an image that newly declares a destination has to take effect on a
// running router, without waiting for a restart. Reading the features service on every call is what
// gives that: the service's polling loop keeps its copy of /features current, so each call observes
// whatever the last poll stored.
func TestTransformerProxyEnabledTracksFeatureChanges(t *testing.T) {
	rt := newProxyTestHandle(t, config.New(), false, true, false)

	require.False(t, rt.transformerProxyEnabled(), "undeclared, and the env is unset")
	require.True(t, rt.transformerProxyEnabled(), "picks up a transformer that now declares it")
	require.False(t, rt.transformerProxyEnabled(), "and one that stops declaring it")
}

// The global Router.transformerProxy key is set to false in the operator base values.yaml, so it is
// always present for every destination. A declared destination must not be switched off by it.
func TestTransformerProxyEnabledIgnoresGlobalDefaultWhenDeclared(t *testing.T) {
	c := config.New()
	c.Set("Router.transformerProxy", "false")

	require.True(t, newProxyTestHandle(t, c, true).transformerProxyEnabled())
}
