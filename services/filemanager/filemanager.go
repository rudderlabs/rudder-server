//go:generate mockgen -destination=../../mocks/services/filemanager/mock_filemanager.go -package mock_filemanager github.com/rudderlabs/rudder-server/services/filemanager FileManagerFactory,FileManager

package filemanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/router/rterror"
)

var (
	DefaultFileManagerFactory FileManagerFactory
	ErrKeyNotFound            = errors.New("NoSuchKey")
)

type FileManagerFactoryT struct{}

type UploadOutput struct {
	Location   string
	ObjectName string
}

type FileManagerFactory interface {
	New(settings *SettingsT) (FileManager, error)
}

type FileObject struct {
	Key          string
	LastModified time.Time
}

// FileManager implements all upload methods
type FileManager interface {
	Upload(context.Context, *os.File, ...string) (UploadOutput, error)
	Download(context.Context, *os.File, string) error
	GetObjectNameFromLocation(string) (string, error)
	GetDownloadKeyFromFileLocation(location string) string
	DeleteObjects(ctx context.Context, keys []string) error
	ListFilesWithPrefix(ctx context.Context, prefix string, maxItems int64) (fileObjects []*FileObject, err error)
	GetConfiguredPrefix() string
	SetTimeout(timeout *time.Duration)
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

func init() {
	DefaultFileManagerFactory = &FileManagerFactoryT{}
}

// New returns FileManager backed by configured provider
func (factory *FileManagerFactoryT) New(settings *SettingsT) (FileManager, error) {
	var timeout time.Duration = time.Minute

	switch settings.Provider {
	case "S3":
		return &S3Manager{
			Config:  GetS3Config(settings.Config),
			Timeout: &timeout,
		}, nil
	case "GCS":
		return &GCSManager{
			Config:  GetGCSConfig(settings.Config),
			Timeout: &timeout,
		}, nil
	case "AZURE_BLOB":
		return &AzureBlobStorageManager{
			Config:  GetAzureBlogStorageConfig(settings.Config),
			Timeout: &timeout,
		}, nil
	case "MINIO":
		return &MinioManager{
			Config:  GetMinioConfig(settings.Config),
			Timeout: &timeout,
		}, nil
	case "DIGITAL_OCEAN_SPACES":
		return &DOSpacesManager{
			Config:  GetDOSpacesConfig(settings.Config),
			Timeout: &timeout,
		}, nil
	}
	return nil, fmt.Errorf("%w: %s", rterror.InvalidServiceProvider, settings.Provider)
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
		credentials, err := os.ReadFile(config.GetEnv("GOOGLE_APPLICATION_CREDENTIALS", ""))
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

func getSafeTimeout(timeout *time.Duration) time.Duration {
	if timeout == nil || *timeout == 0 {
		return time.Second * 120
	}
	return *timeout
}
