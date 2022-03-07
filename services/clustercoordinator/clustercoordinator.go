//go:generate mockgen -destination=../../mocks/services/filemanager/mock_filemanager.go -package mock_filemanager github.com/rudderlabs/rudder-server/services/filemanager FileManagerFactory,FileManager

package clustercoordinator

import (
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
