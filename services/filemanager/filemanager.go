package filemanager

import (
	"errors"
	"os"

	"github.com/rudderlabs/rudder-server/config"
)

type UploadOutput struct {
	Location string
}

// FileManager inplements all upload methods
type FileManager interface {
	Upload(*os.File, ...string) (UploadOutput, error)
	Download(*os.File, string) error
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

// New returns FileManager backed by configured privider
func New(settings *SettingsT) (FileManager, error) {
	switch settings.Provider {
	case "S3":
		return &S3Manager{
			Config: GetS3Config(settings.Config),
		}, nil
	case "GCS":
		return &GCSManager{
			Config: GetGCSConfig(settings.Config),
		}, nil
	case "AZURE_BLOB":
		return &AzureBlobStorageManager{
			Config: GetAzureBlogStorageConfig(settings.Config),
		}, nil
	}
	return nil, errors.New("No provider configured for FileManager")
}

// GetProviderConfigFromEnv returns the provider config
func GetProviderConfigFromEnv() map[string]interface{} {
	providerConfig := make(map[string]interface{})
	provider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
	switch provider {
	case "S3":
		providerConfig["bucketName"] = config.GetEnv("JOBS_BACKUP_BUCKET", "")
	case "AZURE_BLOB":
		providerConfig["containerName"] = config.GetEnv("JOBS_BACKUP_BUCKET", "")
	}
	return providerConfig
}
