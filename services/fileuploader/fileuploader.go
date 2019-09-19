package fileuploader

import (
	"errors"
	"os"
)

// FileUploader inplements all upload methods
type FileUploader interface {
	Upload(file *os.File, prefixes ...string) error
}

// SettingsT sets configuration for FileUploader
type SettingsT struct {
	Provider       string
	AmazonS3Bucket string
}

// NewFileUploader returns FileFileUploader backed by configured privider
func NewFileUploader(settings *SettingsT) (FileUploader, error) {
	switch settings.Provider {
	case "s3":
		return &S3Uploader{
			bucket: settings.AmazonS3Bucket,
		}, nil
	}
	return nil, errors.New("No provider configured for FileUploader")
}
