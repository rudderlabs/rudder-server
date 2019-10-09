package fileuploader

import (
	"errors"
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"os"
)

// FileUploader inplements all upload methods
type FileUploader interface {
	Upload(file *os.File, prefixes ...string) error
}

// SettingsT sets configuration for FileUploader
type SettingsT struct {
	Provider string
	Bucket   string
}

// NewFileUploader returns FileFileUploader backed by configured privider
func NewFileUploader(settings *SettingsT) (FileUploader, error) {
	switch settings.Provider {
	case "s3":
		return &S3Uploader{
			bucket: settings.Bucket,
		}, nil
	case "minio":
		return &MinIOUploader{
			bucket:          settings.Bucket,
			endpoint:        config.GetEnv("MINIO_ENDPOINT", ""),
			accessKeyID:     config.GetEnv("MINIO_ACCESS_KEY", ""),
			secretAccessKey: config.GetEnv("MINIO_SECRET_KEY", ""),
			useSSL:          config.GetBool("MINIO_USE_SSL", false),
		}, nil
	}
	return nil, errors.New("No provider configured for FileUploader: " + settings.Provider)
}

func CreateUploaderForDestination(destination backendconfig.DestinationT) (FileUploader, error) {

	switch destination.DestinationDefinition.Name {
	case "s3":
		return &S3Uploader{
			bucket: destination.Config.(map[string]interface{})["bucketName"].(string),
		}, nil
	case "minio":

		return &MinIOUploader{
			bucket:          destination.Config.(map[string]interface{})["bucketName"].(string),
			endpoint:        destination.Config.(map[string]interface{})["MINIO_ENDPOINT"].(string),
			accessKeyID:     destination.Config.(map[string]interface{})["MINIO_ACCESS_KEY_ID"].(string),
			secretAccessKey: destination.Config.(map[string]interface{})["MINIO_SECRET_ACCESS_KEY"].(string),
			useSSL:          destination.Config.(map[string]interface{})["MINIO_USE_SSL"].(bool),
		}, nil
	}

	return nil, errors.New(fmt.Sprintf(
		"Unknown provider type %s for destination %s",
		destination.DestinationDefinition.Name,
		destination.Name,
	))
}
