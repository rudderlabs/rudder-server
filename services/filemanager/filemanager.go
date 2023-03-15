//go:generate mockgen -destination=../../mocks/services/filemanager/mock_filemanager.go -package mock_filemanager github.com/rudderlabs/rudder-server/services/filemanager FileManagerFactory,FileManager

package filemanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/rterror"
)

var (
	pkgLogger                 logger.Logger
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
	ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) (fileObjects []*FileObject, err error)
	GetConfiguredPrefix() string
	SetTimeout(timeout time.Duration)
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

func init() {
	DefaultFileManagerFactory = &FileManagerFactoryT{}
	pkgLogger = logger.NewLogger().Child("filemanager")
}

// New returns FileManager backed by configured provider
func (*FileManagerFactoryT) New(settings *SettingsT) (FileManager, error) {
	switch settings.Provider {
	case "S3_DATALAKE":
		return NewS3Manager(settings.Config)
	case "S3":
		return NewS3Manager(settings.Config)
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
	return nil, fmt.Errorf("%w: %s", rterror.InvalidServiceProvider, settings.Provider)
}

func GetProviderConfigFromEnv(ctx context.Context, provider string) map[string]interface{} {
	providerConfig := make(map[string]interface{})
	switch provider {

	case "S3":
		providerConfig["bucketName"] = config.GetString("JOBS_BACKUP_BUCKET", "rudder-saas")
		providerConfig["prefix"] = config.GetString("JOBS_BACKUP_PREFIX", "")
		providerConfig["accessKeyID"] = config.GetString("AWS_ACCESS_KEY_ID", "")
		providerConfig["accessKey"] = config.GetString("AWS_SECRET_ACCESS_KEY", "")
		providerConfig["enableSSE"] = config.GetBool("AWS_ENABLE_SSE", false)
		providerConfig["regionHint"] = config.GetString("AWS_S3_REGION_HINT", "us-east-1")
		providerConfig["iamRoleArn"] = config.GetString("BACKUP_IAM_ROLE_ARN", "")
		if providerConfig["iamRoleArn"] != "" {
			backendconfig.DefaultBackendConfig.WaitForConfig(ctx)
			providerConfig["externalId"] = backendconfig.DefaultBackendConfig.Identity().ID()
		}

	case "GCS":
		providerConfig["bucketName"] = config.GetString("JOBS_BACKUP_BUCKET", "rudder-saas")
		providerConfig["prefix"] = config.GetString("JOBS_BACKUP_PREFIX", "")
		credentials, err := os.ReadFile(config.GetString("GOOGLE_APPLICATION_CREDENTIALS", ""))
		if err == nil {
			providerConfig["credentials"] = string(credentials)
		}

	case "AZURE_BLOB":
		providerConfig["containerName"] = config.GetString("JOBS_BACKUP_BUCKET", "rudder-saas")
		providerConfig["prefix"] = config.GetString("JOBS_BACKUP_PREFIX", "")
		providerConfig["accountName"] = config.GetString("AZURE_STORAGE_ACCOUNT", "")
		providerConfig["accountKey"] = config.GetString("AZURE_STORAGE_ACCESS_KEY", "")

	case "MINIO":
		providerConfig["bucketName"] = config.GetString("JOBS_BACKUP_BUCKET", "rudder-saas")
		providerConfig["prefix"] = config.GetString("JOBS_BACKUP_PREFIX", "")
		providerConfig["endPoint"] = config.GetString("MINIO_ENDPOINT", "localhost:9000")
		providerConfig["accessKeyID"] = config.GetString("MINIO_ACCESS_KEY_ID", "minioadmin")
		providerConfig["secretAccessKey"] = config.GetString("MINIO_SECRET_ACCESS_KEY", "minioadmin")
		providerConfig["useSSL"] = config.GetBool("MINIO_SSL", false)

	case "DIGITAL_OCEAN_SPACES":
		providerConfig["bucketName"] = config.GetString("JOBS_BACKUP_BUCKET", "rudder-saas")
		providerConfig["prefix"] = config.GetString("JOBS_BACKUP_PREFIX", "")
		providerConfig["endPoint"] = config.GetString("DO_SPACES_ENDPOINT", "")
		providerConfig["accessKeyID"] = config.GetString("DO_SPACES_ACCESS_KEY_ID", "")
		providerConfig["accessKey"] = config.GetString("DO_SPACES_SECRET_ACCESS_KEY", "")
	}

	return providerConfig
}

// GetProviderConfigForBackupsFromEnv returns the provider config
func GetProviderConfigForBackupsFromEnv(ctx context.Context) map[string]interface{} {
	return GetProviderConfigFromEnv(
		ctx,
		config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"))
}

func getBatchRouterTimeoutConfig(destType string) time.Duration {
	key := "timeout"
	defaultValueInTimescaleUnits := int64(120)
	timeScale := time.Second

	destOverrideFound := config.IsSet("BatchRouter." + destType + "." + key)
	if destOverrideFound {
		return config.GetDuration("BatchRouter."+destType+"."+key, defaultValueInTimescaleUnits, timeScale)
	} else {
		return config.GetDuration("BatchRouter."+key, defaultValueInTimescaleUnits, timeScale)
	}
}

func IterateFilesWithPrefix(ctx context.Context, prefix, startAfter string, maxItems int64, manager *FileManager) *ObjectIterator {
	it := &ObjectIterator{
		ctx:        ctx,
		startAfter: startAfter,
		maxItems:   maxItems,
		prefix:     prefix,
		manager:    manager,
	}
	return it
}

type ObjectIterator struct {
	ctx        context.Context
	err        error
	item       *FileObject
	items      []*FileObject
	manager    *FileManager
	maxItems   int64
	startAfter string
	prefix     string
}

func (it *ObjectIterator) Next() bool {
	var err error
	if len(it.items) == 0 {
		mn := *it.manager
		it.items, err = mn.ListFilesWithPrefix(it.ctx, it.startAfter, it.prefix, it.maxItems)
		if err != nil {
			it.err = err
			return false
		}
		if len(it.items) > 0 {
			pkgLogger.Infof(`Fetched files list from %v (lastModifiedAt: %v) to %v (lastModifiedAt: %v)`, it.items[0].Key, it.items[0].LastModified, it.items[len(it.items)-1].Key, it.items[len(it.items)-1].LastModified)
		}
	}

	if len(it.items) > 0 {
		it.item = it.items[0]
		it.items = it.items[1:]
		return true
	}
	return false
}

func (it *ObjectIterator) Get() *FileObject {
	return it.item
}

func (it *ObjectIterator) Err() error {
	return it.err
}
