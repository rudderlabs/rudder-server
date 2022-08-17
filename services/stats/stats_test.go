package stats_test

import (
	"os"
	"testing"

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
	t.Run("dedicated deployment type, dont remove workspaceId tag", func(t *testing.T) {
		os.Setenv("DEPLOYMENT_TYPE", string(deployment.DedicatedType))
		tags := stats.Tags{
			"b":           "value1",
			"workspaceId": "value2",
		}
		expectedTags := stats.Tags{
			"b":           "value1",
			"workspaceId": "value2",
		}
		cleanedTags := stats.CleanupTagsBasedOnDeploymentType(tags, "workspaceId")
		require.Equal(t, expectedTags, cleanedTags)
	})

	t.Run("multitenant deployment type & not namespaced, remove workspaceId tag", func(t *testing.T) {
		os.Setenv("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
		tags := stats.Tags{
			"b":           "value1",
			"workspaceId": "value2",
		}
		expectedTags := stats.Tags{
			"b": "value1",
		}
		cleanedTags := stats.CleanupTagsBasedOnDeploymentType(tags, "workspaceId")
		require.Equal(t, expectedTags, cleanedTags)
	})

	t.Run("multitenant deployment type & pro namespaced, dont remove workspaceId tag", func(t *testing.T) {
		os.Setenv("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
		os.Setenv("WORKSPACE_NAMESPACE", "pro")
		tags := stats.Tags{
			"b":           "value1",
			"workspaceId": "value2",
		}
		expectedTags := stats.Tags{
			"b":           "value1",
			"workspaceId": "value2",
		}
		cleanedTags := stats.CleanupTagsBasedOnDeploymentType(tags, "workspaceId")
		require.Equal(t, expectedTags, cleanedTags)
	})

	t.Run("multitenant deployment type & free-tier-us namespaced, remove workspaceId tag", func(t *testing.T) {
		os.Setenv("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
		os.Setenv("WORKSPACE_NAMESPACE", "free-tier-us")
		tags := stats.Tags{
			"b":           "value1",
			"workspaceId": "value2",
		}
		expectedTags := stats.Tags{
			"b": "value1",
		}
		cleanedTags := stats.CleanupTagsBasedOnDeploymentType(tags, "workspaceId")
		require.Equal(t, expectedTags, cleanedTags)
	})
}
