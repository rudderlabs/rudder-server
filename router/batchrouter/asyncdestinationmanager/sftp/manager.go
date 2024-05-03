package sftp

import (
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sftp"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (*DefaultManager) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
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

	result := gjson.ParseBytes(destConfigJSON)
	uploadFilePath := result.Get("filePath").String()
	uploadFilePath = getUploadFilePath(uploadFilePath)
	fileFormat := result.Get("fileFormat").String()

	// Generate temporary file based on the destination's file format
	jsonOrCSVFilePath, err := generateFile(textFilePath, fileFormat)
	if err != nil {
		return generateErrorOutput(fmt.Sprintf("error generating temporary file: %v", err.Error()), asyncDestStruct.ImportingJobIDs, destinationID)
	}

	d.logger.Debugn("File Upload Started", obskit.DestinationID(destinationID))

	// Upload file
	err = d.FileManager.Upload(jsonOrCSVFilePath, uploadFilePath)
	if err != nil {
		return common.AsyncUploadOutput{
			DestinationID: destinationID,
			FailedCount:   len(asyncDestStruct.ImportingJobIDs),
			FailedJobIDs:  asyncDestStruct.ImportingJobIDs,
			FailedReason:  fmt.Sprintf("error uploading file to destination: %v", err.Error()),
		}
	}

	d.logger.Debugn("File Upload Finished", obskit.DestinationID(destinationID))

	return common.AsyncUploadOutput{
		DestinationID:   destinationID,
		SucceededJobIDs: asyncDestStruct.ImportingJobIDs,
		SuccessResponse: "File Upload Success",
	}
}

func newDefaultManager(fileManager sftp.FileManager) *DefaultManager {
	return &DefaultManager{
		FileManager: fileManager,
		logger:      logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("SFTP").Child("DefaultManager"),
	}
}

func newInternalManager(destination *backendconfig.DestinationT) (common.AsyncUploadAndTransformManager, error) {
	sshConfig, err := createSSHConfig(destination)
	if err != nil {
		return nil, fmt.Errorf("creating SSH config: %w", err)
	}
	sshClient, err := sftp.NewSSHClient(sshConfig)
	if err != nil {
		return nil, fmt.Errorf("creating SSH client: %w", err)
	}

	fileManager, err := sftp.NewFileManager(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, fmt.Errorf("creating file manager: %w", err)
	}

	return newDefaultManager(fileManager), nil
}

func NewManager(destination *backendconfig.DestinationT) (common.AsyncDestinationManager, error) {
	sftpManager, err := newInternalManager(destination)
	if err != nil {
		return nil, err
	}
	return common.SimpleAsyncDestinationManager{UploaderAndTransformer: sftpManager}, nil
}
