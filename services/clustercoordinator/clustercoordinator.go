//go:generate mockgen -destination=../../mocks/services/filemanager/mock_filemanager.go -package mock_filemanager github.com/rudderlabs/rudder-server/services/filemanager FileManagerFactory,FileManager

package clustercoordinator

import (
	"time"

	"github.com/rudderlabs/rudder-server/config"
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

// GetProviderConfigFromEnv returns the provider config
func GetProviderConfigFromEnv() map[string]interface{} {
	providerConfig := make(map[string]interface{})
	provider := config.GetEnv("CLUSTER_COORDINATOR", "")
	switch provider {
	case "ETCD":
		providerConfig["bucketName"] = config.GetEnv("JOBS_BACKUP_BUCKET", "")
		providerConfig["prefix"] = config.GetEnv("JOBS_BACKUP_PREFIX", "")
		providerConfig["accessKeyID"] = config.GetEnv("AWS_ACCESS_KEY_ID", "")
		providerConfig["accessKey"] = config.GetEnv("AWS_SECRET_ACCESS_KEY", "")
		providerConfig["enableSSE"] = config.GetEnvAsBool("AWS_ENABLE_SSE", false)
	default:
		providerConfig["bucketName"] = config.GetEnv("JOBS_BACKUP_BUCKET", "")
		providerConfig["prefix"] = config.GetEnv("JOBS_BACKUP_PREFIX", "")
		providerConfig["endPoint"] = config.GetEnv("DO_SPACES_ENDPOINT", "")
		providerConfig["accessKeyID"] = config.GetEnv("DO_SPACES_ACCESS_KEY_ID", "")
		providerConfig["accessKey"] = config.GetEnv("DO_SPACES_SECRET_ACCESS_KEY", "")
	}
	return providerConfig
}
