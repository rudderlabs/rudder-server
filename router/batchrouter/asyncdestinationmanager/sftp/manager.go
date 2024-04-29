package sftp

import (
	stdjson "encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	sftp "github.com/rudderlabs/rudder-go-kit/sftp"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/tidwall/gjson"
)

func (d *DefaultManager) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(job, func(payload stdjson.RawMessage) string {
		return string(payload)
	}), nil
}

// Upload uploads the data to the destination and marks all jobs to be completed
func (d *DefaultManager) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	textFilePath := asyncDestStruct.FileName
	destinationID := destination.ID
	destConfigJSON, err := json.Marshal(destination.Config)
	if err != nil {
		return generateErrorOutput(fmt.Sprintf("error marshalling destination config: %v", err.Error()), asyncDestStruct.ImportingJobIDs, destinationID)
	}

	uploadFilePath := gjson.Get(string(destConfigJSON), "filePath").String()
	uploadFilePath = getUploadFilePath(uploadFilePath)
	fileFormat := gjson.Get(string(destConfigJSON), "fileFormat").String()

	// Generate temporary file based on the destination's file format
	jsonOrCSVFilePath, err := generateFile(textFilePath, fileFormat)
	if err != nil {
		return generateErrorOutput(fmt.Sprintf("error generating temporary file: %v", err.Error()), asyncDestStruct.ImportingJobIDs, destinationID)

	}

	d.logger.Debugf("[Async Destination Manager] File Upload Started for Dest Type %v", destination.DestinationDefinition.Name)

	// Upload file
	err = d.FileManager.Upload(jsonOrCSVFilePath, uploadFilePath)
	if err != nil {
		return generateErrorOutput(fmt.Sprintf("error uploading file to destination: %v", err.Error()), asyncDestStruct.ImportingJobIDs, destinationID)
	}

	d.logger.Debugf("[Async Destination Manager] File Upload Finished for Dest Type %v", destination.DestinationDefinition.Name)

	return common.AsyncUploadOutput{
		DestinationID:   destinationID,
		SucceededJobIDs: asyncDestStruct.ImportingJobIDs,
		SuccessResponse: "File Upload Success",
	}
}

func NewDefaultManager(fileManager sftp.FileManager) *DefaultManager {
	return &DefaultManager{
		FileManager: fileManager,
		logger:      logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("SFTP").Child("DefaultManager"),
	}
}

func newInternalManager(destination *backendconfig.DestinationT) (common.AsyncUploadAndTransformManager, error) {
	sshConfig, err := createSSHConfig(destination)
	if err != nil {
		return nil, err
	}

	sshClient, err := sftp.NewSSHClient(sshConfig)
	if err != nil {
		return nil, err
	}

	fileManager, err := sftp.NewFileManager(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, err
	}

	return NewDefaultManager(fileManager), nil
}

func NewManager(destination *backendconfig.DestinationT) (common.AsyncDestinationManager, error) {
	sftpManager, err := newInternalManager(destination)
	if err != nil {
		return nil, err
	}
	return common.SimpleAsyncDestinationManager{UploaderAndTransformer: sftpManager}, nil
}