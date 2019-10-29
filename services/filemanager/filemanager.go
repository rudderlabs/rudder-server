package filemanager

import (
	"errors"
	"os"
)

// FileManager inplements all upload methods
type FileManager interface {
	Upload(*os.File, ...string) error
	Download(*os.File, string) error
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Bucket   string
}

// New returns FileManager backed by configured privider
func New(settings *SettingsT) (FileManager, error) {
	switch settings.Provider {
	case "S3":
		return &S3Manager{
			Bucket: settings.Bucket,
		}, nil
	}
	return nil, errors.New("No provider configured for FileManager")
}
