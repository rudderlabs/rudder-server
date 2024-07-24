package sftp

import (
	"fmt"
	"os"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sftp"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (*defaultManager) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

// Upload uploads the data to the destination and marks all jobs to be completed
func (d *defaultManager) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	startTime := time.Now()
	destination := asyncDestStruct.Destination
	textFilePath := asyncDestStruct.FileName
	destinationID := destination.ID
	destType := destination.DestinationDefinition.Name
	destConfigJSON, err := json.Marshal(destination.Config)
	if err != nil {
		return generateErrorOutput(fmt.Sprintf("error marshalling destination config: %v", err.Error()), asyncDestStruct.ImportingJobIDs, destinationID)
	}

	jobRunID := gjson.GetBytes(asyncDestStruct.OriginalJobParameters[0], "source_job_run_id").String()
	metadata := map[string]any{
		"destinationID":  destinationID,
		"sourceJobRunID": jobRunID,
	}

	result := gjson.ParseBytes(destConfigJSON)
	uploadFilePath := result.Get("filePath").String()
	uploadFilePath = getUploadFilePath(uploadFilePath, metadata)
	fileFormat := result.Get("fileFormat").String()

	// Generate temporary file based on the destination's file format
	jsonOrCSVFilePath, err := generateFile(textFilePath, fileFormat)
	if err != nil {
		return generateErrorOutput(fmt.Sprintf("error generating temporary file: %v", err.Error()), asyncDestStruct.ImportingJobIDs, destinationID)
	}
	defer func() {
		_ = os.Remove(jsonOrCSVFilePath)
	}()

	fileInfo, err := os.Stat(textFilePath)
	if err != nil {
		return generateErrorOutput(fmt.Sprintf("error getting file info: %v", err.Error()), asyncDestStruct.ImportingJobIDs, destinationID)
	}
	statLabels := stats.Tags{
		"module":   "batch_router",
		"destType": destType,
	}

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, statLabels)
	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType, statLabels)
	eventsSuccessStat := stats.Default.NewTaggedStat("success_job_count", stats.CountType, statLabels)

	payloadSizeStat.Observe(float64(fileInfo.Size()))
	d.logger.Debugn("File Upload Started", obskit.DestinationID(destinationID))

	// Upload file
	err = d.FileManager.Upload(jsonOrCSVFilePath, uploadFilePath)
	if err != nil {
		return generateErrorOutput(fmt.Sprintf("error uploading file to destination: %v", err.Error()), asyncDestStruct.ImportingJobIDs, destinationID)
	}

	d.logger.Debugn("File Upload Finished", obskit.DestinationID(destinationID))
	uploadTimeStat.Since(startTime)
	eventsSuccessStat.Count(len(asyncDestStruct.ImportingJobIDs))
	return common.AsyncUploadOutput{
		DestinationID:   destinationID,
		SucceededJobIDs: asyncDestStruct.ImportingJobIDs,
		SuccessResponse: "File Upload Success",
	}
}

func newDefaultManager(fileManager sftp.FileManager) *defaultManager {
	return &defaultManager{
		FileManager: fileManager,
		logger:      logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("SFTP").Child("Manager"),
	}
}

func newInternalManager(destination *backendconfig.DestinationT) (common.AsyncUploadAndTransformManager, error) {
	sshConfig, err := createSSHConfig(destination)
	if err != nil {
		return nil, fmt.Errorf("creating SSH config: %w", err)
	}

	fileManager, err := sftp.NewFileManager(sshConfig, sftp.WithRetryOnIdleConnection())
	if err != nil {
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
