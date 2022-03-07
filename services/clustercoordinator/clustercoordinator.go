//go:generate mockgen -destination=../../mocks/services/clustercoordinator/mock_clustercoordinator.go -package mock_clustercoordinator github.com/rudderlabs/rudder-server/services/clustercoordinator ClusterManagerFactory,ClusterManager

package clustercoordinator

import (
	"context"
	"time"
)

var (
	DefaultClusterManagerFactory ClusterManagerFactory
)

type ClusterManagerFactoryT struct{}

type UploadOutput struct {
	Location   string
	ObjectName string
}

type ClusterManagerFactory interface {
	New(provider string) (ClusterManager, error)
}

type FileObject struct {
	Key          string
	LastModified time.Time
}

// FileManager implements all upload methods
type ClusterManager interface {
	Get(ctx context.Context, key string) (string, error)
	Watch(ctx context.Context, key string) chan interface{}
	Put(ctx context.Context, key string, value string) error
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

func init() {
	DefaultClusterManagerFactory = &ClusterManagerFactoryT{}
}

// Deprecated: Use an instance of FileManagerFactory instead
func New(provider string) (ClusterManager, error) {
	return DefaultClusterManagerFactory.New(provider)
}

// New returns FileManager backed by configured provider
func (factory *ClusterManagerFactoryT) New(provider string) (ClusterManager, error) {
	switch provider {
	case "ETCD":
		return &ETCDManager{
			Config: GetETCDConfig(),
		}, nil
	default:
		return &NOOPManager{
			Config: GetNOOPConfig(),
		}, nil
	}
}
