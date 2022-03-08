package clustercoordinator

import (
	"context"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
)

var (
	newWorkspaceKey string
	migrationKey    string
)

func init() {
	newWorkspaceKey = config.GetString("NEW_WORKSPACE_KEY", config.GetNamespaceIdentifier()+"/SERVER/"+config.GetInstanceID()+"/workspaces")
	migrationKey = config.GetString("MIGRATION_KEY", config.GetNamespaceIdentifier()+"/SERVER/"+config.GetInstanceID()+"/mode")
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdHandler, err := New("ETCD")
	if err != nil {
		return
	}
	go newWorkspaceSequence(ctx, etcdHandler)
	// go migrationSequence()
}

func newWorkspaceSequence(ctx context.Context, etcdHandler ClusterManager) {
	respChannel := etcdHandler.WatchForWorkspaces(ctx, newWorkspaceKey)
	for workspaces := range respChannel {
		backendconfig.DefaultBackendConfig.StopPolling()
		backendconfig.DefaultBackendConfig.StartPolling(workspaces)
		backendconfig.DefaultBackendConfig.WaitForConfig(ctx)
	}
}
