package asyncdestinationmanager

import (
	"errors"
)

var (
	DefaultFileManagerFactory AsyncFileManagerFactory
)

type AsyncFileManagerFactoryT struct{}

type UploadOutput struct {
	Location   string
	ObjectName string
}

type AsyncFileManagerFactory interface {
	New(settings *SettingsT) (AsyncFileManager, error)
}

// FileManager inplements all upload methods
type AsyncFileManager interface {
	Upload(url string, method string, filePath string)
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

func init() {
	DefaultFileManagerFactory = &AsyncFileManagerFactoryT{}
}

// New returns FileManager backed by configured provider
func (factory *AsyncFileManagerFactoryT) New(settings *SettingsT) (AsyncFileManager, error) {
	switch settings.Provider {
	case "MARKETO_BULK_UPLOAD":
		return &MarketoManager{
			Config: GetMarketoConfig(settings.Config),
		}, nil
	}
	return nil, errors.New("no provider configured for FileManager")
}
