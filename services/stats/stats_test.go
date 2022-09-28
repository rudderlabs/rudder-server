package stats_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/stretchr/testify/require"
)

func TestTags(t *testing.T) {
	tags := stats.Tags{
		"b": "value1",
		"a": "value2",
	}

	t.Run("strings method", func(t *testing.T) {
		for i := 0; i < 100; i++ { // just making sure we are not just lucky with the order
			require.Equal(t, []string{"a", "value2", "b", "value1"}, tags.Strings())
		}
	})

	t.Run("string method", func(t *testing.T) {
		require.Equal(t, "a,value2,b,value1", tags.String())
	})

	t.Run("special character replacement", func(t *testing.T) {
		specialTags := stats.Tags{
			"b:1": "value1:1",
			"a:1": "value2:2",
		}
		require.Equal(t, []string{"a-1", "value2-2", "b-1", "value1-1"}, specialTags.Strings())
	})

	t.Run("empty tags", func(t *testing.T) {
		emptyTags := stats.Tags{}
		require.Nil(t, emptyTags.Strings())
		require.Equal(t, "", emptyTags.String())
	})
}

func TestCleanupTagsBasedOnDeploymentType(t *testing.T) {
	testCases := []struct {
		name           string
		deploymentType deployment.Type
		namespace      string
		shouldFilter   bool
	}{
		{
			name:         "dedicated deployment type",
			shouldFilter: false,

			deploymentType: deployment.DedicatedType,
		},
		{
			name:         "multitenant deployment type & not namespaced",
			shouldFilter: false,

			deploymentType: deployment.MultiTenantType,
		},
		{
			name:         "multitenant deployment type & pro namespaced",
			shouldFilter: false,

			deploymentType: deployment.MultiTenantType,
			namespace:      "pro",
		},
		{
			name:         "multitenant deployment type & free namespaced",
			shouldFilter: true,

			deploymentType: deployment.MultiTenantType,
			namespace:      "free",
		},
	}

	filterKeys := []string{"workspaceId", "sourceID", "destID", "missing"}

	withoutFilter := stats.Tags{
		"b":           "value1",
		"workspaceId": "value2",
		"sourceID":    "value3",
		"destID":      "value4",
	}

	withFilter := stats.Tags{
		"b": "value1",
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tags := stats.Tags{
				"b":           "value1",
				"workspaceId": "value2",
				"sourceID":    "value3",
				"destID":      "value4",
			}

			t.Log(tc.namespace)
			config.Default.Set("DEPLOYMENT_TYPE", tc.deploymentType)
			if tc.namespace != "" {
				config.Default.Set("WORKSPACE_NAMESPACE", tc.namespace)
			}

			cleanedTags := stats.CleanupTagsBasedOnDeploymentType(tags, filterKeys...)

			if tc.shouldFilter {
				require.Equal(t, withFilter, cleanedTags)
			} else {
				require.Equal(t, withoutFilter, cleanedTags)
			}
		})
	}

}
