package sftp

import (
	"errors"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// DefaultManager is the default manager for SFTP
// TODO: Implement this
type DefaultManager struct{}

// Upload uploads the data to the destination and marks all jobs to be completed
// TODO: Implement this
func (m *DefaultManager) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	return common.AsyncUploadOutput{}
}

func NewDefaultManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncUploadDestinationManager, error) {
	return &DefaultManager{}, nil
}

func newInternalManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncUploadDestinationManager, error) {
	switch destination.DestinationDefinition.Name {
	case "SFTP":
		return NewDefaultManager(destination, backendConfig)
	}
	return nil, errors.New("invalid sftp destination type")
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncDestinationManager, error) {
	sftpManager, err := newInternalManager(destination, backendConfig)
	if err != nil {
		return nil, err
	}
	return common.SimpleAsyncDestinationManager{Uploader: sftpManager}, nil
}
