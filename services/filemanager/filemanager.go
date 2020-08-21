package filemanager

import (
	"errors"
	"io/ioutil"
	"os"

	"github.com/rudderlabs/rudder-server/config"
)

type UploadOutput struct {
	Location   string
	ObjectName string
}

// FileManager inplements all upload methods
type FileManager interface {
	Upload(*os.File, ...string) (UploadOutput, error)
	Download(*os.File, string) error
	GetObjectNameFromLocation(string) (string, error)
	GetDownloadKeyFromFileLocation(location string) string
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

// New returns FileManager backed by configured provider
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
	case "MINIO":
		return &MinioManager{
			Config: GetMinioConfig(settings.Config),
		}, nil
	case "DIGITAL_OCEAN_SPACES":
		return &DOSpacesManager{
			Config: GetDOSpacesConfig(settings.Config),
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
		providerConfig["prefix"] = config.GetEnv("JOBS_BACKUP_PREFIX", "")
		providerConfig["accessKeyID"] = config.GetEnv("AWS_ACCESS_KEY_ID", "")
		providerConfig["accessKey"] = config.GetEnv("AWS_SECRET_ACCESS_KEY", "")
		providerConfig["enableSSE"] = config.GetEnvAsBool("AWS_ENABLE_SSE", false)
	case "GCS":
		providerConfig["bucketName"] = config.GetEnv("JOBS_BACKUP_BUCKET", "")
		providerConfig["prefix"] = config.GetEnv("JOBS_BACKUP_PREFIX", "")
		credentials, err := ioutil.ReadFile(config.GetEnv("GOOGLE_APPLICATION_CREDENTIALS", ""))
		if err == nil {
			providerConfig["credentials"] = string(credentials)
		}
	case "AZURE_BLOB":
		providerConfig["containerName"] = config.GetEnv("JOBS_BACKUP_BUCKET", "")
		providerConfig["prefix"] = config.GetEnv("JOBS_BACKUP_PREFIX", "")
		providerConfig["accountName"] = config.GetEnv("AZURE_STORAGE_ACCOUNT", "")
		providerConfig["accountKey"] = config.GetEnv("AZURE_STORAGE_ACCESS_KEY", "")
	case "MINIO":
		providerConfig["bucketName"] = config.GetEnv("JOBS_BACKUP_BUCKET", "")
		providerConfig["prefix"] = config.GetEnv("JOBS_BACKUP_PREFIX", "")
		providerConfig["endPoint"] = config.GetEnv("MINIO_ENDPOINT", "localhost:9000")
		providerConfig["accessKeyID"] = config.GetEnv("MINIO_ACCESS_KEY_ID", "minioadmin")
		providerConfig["secretAccessKey"] = config.GetEnv("MINIO_SECRET_ACCESS_KEY", "minioadmin")
		providerConfig["useSSL"] = config.GetEnvAsBool("MINIO_SSL", false)
	case "DIGITAL_OCEAN_SPACES":
		providerConfig["bucketName"] = config.GetEnv("JOBS_BACKUP_BUCKET", "")
		providerConfig["prefix"] = config.GetEnv("JOBS_BACKUP_PREFIX", "")
		providerConfig["endPoint"] = config.GetEnv("DO_SPACES_ENDPOINT", "")
		providerConfig["accessKeyID"] = config.GetEnv("DO_SPACES_ACCESS_KEY_ID", "")
		providerConfig["accessKey"] = config.GetEnv("DO_SPACES_SECRET_ACCESS_KEY", "")
	}
	return providerConfig
}
