package bingads

import (
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	bingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
)

type Client struct {
	URL    string
	client *http.Client
}
type BingAdsBulkUploader struct {
	destName string
	service  bingads.BulkServiceI
	logger   logger.Logger
	client   Client
}
type User struct {
	Email       string `json:"email"`
	HashedEmail string `json:"hashedEmail"`
}
type Message struct {
	List   []User `json:"List"`
	Action string `json:"Action"`
}
type Metadata struct {
	JobID int64 `json:"job_id"`
}

// This struct represent each line of the text file created by the batchrouter
type Data struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
}

type DestinationConfig struct {
	AudienceId               string   `json:"audienceId"`
	CustomerAccountId        string   `json:"customerAccountId"`
	CustomerId               string   `json:"customerId"`
	OneTrustCookieCategories []string `json:"oneTrustCookieCategories"`
	RudderAccountId          string   `json:"rudderAccountId"`
}

type secretStruct struct {
	AccessToken     string
	RefreshToken    string
	Developer_token string
	ExpirationDate  string
}

type tokenSource struct {
	workspaceID     string
	destinationName string
	accountID       string
	oauthClient     oauth.Authorizer
}

func NewBingAdsBulkUploader(name string, service bingads.BulkServiceI, client *Client) *BingAdsBulkUploader {
	return &BingAdsBulkUploader{
		destName: name,
		service:  service,
		logger:   logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("BingAds").Child("BingAdsBulkUploader"),
		client:   *client,
	}
}

func newManagerInternal(destination *backendconfig.DestinationT, oauthClient oauth.Authorizer) (*BingAdsBulkUploader, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("Error in marshalling destination config: %v", err)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("Error in unmarshalling destination config: %v", err)
	}

	tokenSource := tokenSource{
		workspaceID:     destination.WorkspaceID,
		destinationName: destination.Name,
		accountID:       destConfig.RudderAccountId,
		oauthClient:     oauthClient,
	}
	secret, err := tokenSource.generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate oauth token: %v", err)
	}
	sessionConfig := bingads.SessionConfig{
		DeveloperToken: secret.Developer_token,
		AccountId:      destConfig.CustomerAccountId,
		CustomerId:     destConfig.CustomerId,
		HTTPClient:     http.DefaultClient,
		TokenSource:    &tokenSource,
	}
	session := bingads.NewSession(sessionConfig)

	clientNew := Client{}
	bingads := NewBingAdsBulkUploader(destination.DestinationDefinition.Name, bingads.NewBulkService(session), &clientNew)
	return bingads, nil
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (*BingAdsBulkUploader, error) {
	oauthClient := oauth.NewOAuthErrorHandler(backendConfig)
	return newManagerInternal(destination, oauthClient)
}

/*
This function create at most 3 zip files from the text file created by the batchrouter
It takes the text file path as input and returns the zip file path
The maximum size of the zip file is 100MB, if the size of the zip file exceeds 100MB then the job is marked as failed
*/
func (b *BingAdsBulkUploader) Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destConfig := DestinationConfig{}
	jsonConfig, _ := json.Marshal(destination.Config)
	_ = json.Unmarshal(jsonConfig, &destConfig)
	var failedJobs []int64
	var successJobs []int64
	var concatImpId string
	var concatError string
	filePaths, successJobIDs, failedJobIds, err := b.CreateZipFile(asyncDestStruct.FileName, destConfig.AudienceId)
	uploadRetryableStat := stats.Default.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	uploadRetryableStat.Count(len(failedJobIds[0]) + len(failedJobIds[1]) + len(failedJobIds[2]))
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while transforming the file. %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	for index, path := range filePaths {
		_, err := os.Stat(path)
		if err != nil {
			continue
		}
		urlResp, err := b.service.GetBulkUploadUrl()
		if err != nil {
			b.logger.Error("Error in getting bulk upload url: %v", err)
			failedJobs = append(append(failedJobs, successJobIDs[index]...), failedJobIds[index]...)
			concatError += fmt.Sprintf("%v:error in getting bulk upload url: %v", index, err) + ","
			continue
		}

		if urlResp.UploadUrl == "" || urlResp.RequestId == "" {
			b.logger.Error(`{"error" : "getting empty string in upload url or request id"}`)
			failedJobs = append(append(failedJobs, successJobIDs[index]...), failedJobIds[index]...)
			concatError += fmt.Sprintf("%v:getting empty string in upload url or request id", index) + ","
			continue
		}

		uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})

		startTime := time.Now()
		uploadBulkFileResp, errorDuringUpload := b.service.UploadBulkFile(urlResp.UploadUrl, filePaths[index])
		uploadTimeStat.Since(startTime)

		err = os.Remove(filePaths[index])
		if err != nil {
			b.logger.Error("Error in removing zip file: %v", err)
			// To do add an alert here
		}
		if errorDuringUpload != nil {
			b.logger.Error("error in uploading the bulk file: %v", errorDuringUpload)
			failedJobs = append(append(failedJobs, successJobIDs[index]...), failedJobIds[index]...)
			concatError += fmt.Sprintf("%v:error in uploading the bulk file: %v", index, errorDuringUpload) + ","
			continue
		}

		if uploadBulkFileResp.RequestId == "" || uploadBulkFileResp.TrackingId == "" {
			failedJobs = append(append(failedJobs, successJobIDs[index]...), failedJobIds[index]...)
			concatError += fmt.Sprintf("%d:getting empty string in upload url or request id", index) + ","
			continue
		}
		concatImpId += uploadBulkFileResp.RequestId + ","
		failedJobs = append(failedJobs, failedJobIds[index]...)
		successJobs = append(successJobs, successJobIDs[index]...)
	}

	var parameters common.ImportParameters
	parameters.ImportId = concatImpId
	importParameters, err := json.Marshal(parameters)
	if err != nil {
		b.logger.Error("Errored in Marshalling parameters" + err.Error())
	}
	concatError = `{"error":"` + concatError + `"}`
	return common.AsyncUploadOutput{
		ImportingJobIDs:     successJobs,
		FailedJobIDs:        append(asyncDestStruct.FailedJobIDs, failedJobs...),
		FailedReason:        concatError,
		ImportingParameters: stdjson.RawMessage(importParameters),
		ImportingCount:      len(successJobs),
		FailedCount:         len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
		DestinationID:       destination.ID,
	}
}

func (b *BingAdsBulkUploader) PollSingleImport(requestId string) (common.PollStatusResponse, int) {
	var resp common.PollStatusResponse
	var statusCode int
	uploadStatusResp, err := b.service.GetBulkUploadStatus(requestId)
	if err != nil {
		resp = common.PollStatusResponse{
			Success:        false,
			StatusCode:     400,
			HasFailed:      true,
			HasWarning:     false,
			FailedJobsURL:  "",
			WarningJobsURL: "",
			OutputFilePath: "",
		}
		// needs to be retried
		statusCode = 500
		return resp, statusCode
	}
	var allSuccessPercentage int = 100
	if uploadStatusResp.PercentComplete == int64(allSuccessPercentage) && uploadStatusResp.RequestStatus == "Completed" {
		// all successful events, do not need to download the file.
		resp = common.PollStatusResponse{
			Success:        true,
			StatusCode:     200,
			HasFailed:      false,
			HasWarning:     false,
			FailedJobsURL:  "",
			WarningJobsURL: "",
			OutputFilePath: "",
		}
		statusCode = 200
	} else if uploadStatusResp.PercentComplete == int64(allSuccessPercentage) && uploadStatusResp.RequestStatus == "CompletedWithErrors" {
		filePaths, err := extractUploadStatusFilePath(uploadStatusResp.ResultFileUrl, requestId)
		if err != nil {
			resp = common.PollStatusResponse{
				Success:        false,
				StatusCode:     400,
				HasFailed:      true,
				HasWarning:     false,
				FailedJobsURL:  "",
				WarningJobsURL: "",
				OutputFilePath: "",
			}
			statusCode = 400
			return resp, statusCode
		}

		// extracting file paths
		var outputPath string
		for _, filePath := range filePaths {
			outputPath = filePath
		}
		resp = common.PollStatusResponse{
			Success:        true,
			StatusCode:     200,
			HasFailed:      true,
			HasWarning:     false,
			FailedJobsURL:  "",
			WarningJobsURL: "",
			OutputFilePath: outputPath,
		}
		statusCode = 200
	} else if uploadStatusResp.RequestStatus == "FileUploaded" ||
		uploadStatusResp.RequestStatus == "InProgress" ||
		uploadStatusResp.RequestStatus == "PendingFileUpload" {
		resp = common.PollStatusResponse{
			Success:        true,
			StatusCode:     200,
			HasFailed:      false,
			HasWarning:     false,
			FailedJobsURL:  "",
			WarningJobsURL: "",
			OutputFilePath: "",
		}
		// needs to be retried to fetch the final status
		statusCode = 500
		return resp, statusCode
	} else {
		/* if uploadStatusResp.RequestStatus == "Failed" || uploadStatusResp.RequestStatus == "UploadFileRowCountExceeded" || uploadStatusResp.RequestStatus == "UploadFileFormatNotSupported" {
		 */
		resp = common.PollStatusResponse{
			Success:        false,
			StatusCode:     400,
			HasFailed:      true,
			HasWarning:     false,
			FailedJobsURL:  "",
			WarningJobsURL: "",
			OutputFilePath: "",
		}
		statusCode = 400
	}
	return resp, statusCode
}

func (b *BingAdsBulkUploader) Poll(pollInput common.AsyncPoll) (common.PollStatusResponse, int) {
	fmt.Println("Polling Bing Ads")
	var cumulativeResp common.PollStatusResponse
	var statusCode int
	requestIdsArray := common.GenerateArrayOfStrings(pollInput.ImportId)
	for _, requestId := range requestIdsArray {
		resp, status := b.PollSingleImport(requestId)
		if status != 200 {
			cumulativeResp = resp
			statusCode = status
			break
		}
		cumulativeResp = common.PollStatusResponse{
			Success:        resp.Success,
			StatusCode:     200,
			HasFailed:      cumulativeResp.HasFailed || resp.HasFailed,
			HasWarning:     cumulativeResp.HasWarning || resp.HasWarning,
			FailedJobsURL:  "",
			WarningJobsURL: "",
			OutputFilePath: cumulativeResp.OutputFilePath + resp.OutputFilePath + ",",
		}
		statusCode = status
	}

	return cumulativeResp, statusCode
}

func (b *BingAdsBulkUploader) GetUploadStatsOfSingleImport(filePath string) (common.GetUploadStatsResponse, int) {
	records := ReadPollResults(filePath)
	status := "200"
	clientIDErrors := ProcessPollStatusData(records)
	eventStatsResponse := common.GetUploadStatsResponse{
		Status: status,
		Metadata: common.EventStatMeta{
			FailedKeys:    GetFailedKeys(clientIDErrors),
			ErrFailed:     nil,
			WarningKeys:   []int64{},
			ErrWarning:    nil,
			SucceededKeys: []int64{},
			ErrSuccess:    nil,
			FailedReasons: GetFailedReasons(clientIDErrors),
		},
	}

	eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	eventsAbortedStat.Count(len(eventStatsResponse.Metadata.FailedKeys))

	eventsSuccessStat := stats.Default.NewTaggedStat("success_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	eventsSuccessStat.Count(len(eventStatsResponse.Metadata.SucceededKeys))

	return eventStatsResponse, 200
}

func (b *BingAdsBulkUploader) GetUploadStats(UploadStatsInput common.FetchUploadJobStatus) (common.GetUploadStatsResponse, int) {
	// considering importing jobs are the primary list of jobs sent
	// making an array of those jobIds
	importList := UploadStatsInput.ImportingList
	var initialEventList []int64
	for _, job := range importList {
		initialEventList = append(initialEventList, job.JobID)
	}
	var eventStatsResponse common.GetUploadStatsResponse
	filePaths := common.GenerateArrayOfStrings(UploadStatsInput.OutputFilePath)
	for _, filePath := range filePaths {
		response, _ := b.GetUploadStatsOfSingleImport(filePath)
		eventStatsResponse = common.GetUploadStatsResponse{
			Status: response.Status,
			Metadata: common.EventStatMeta{
				FailedKeys:    append(eventStatsResponse.Metadata.FailedKeys, response.Metadata.FailedKeys...),
				ErrFailed:     nil,
				WarningKeys:   []int64{},
				ErrWarning:    nil,
				SucceededKeys: []int64{},
				ErrSuccess:    nil,
				FailedReasons: common.MergeMaps(eventStatsResponse.Metadata.FailedReasons, response.Metadata.FailedReasons),
			},
		}

	}

	// filtering out failed jobIds from the total array of jobIds
	eventStatsResponse.Metadata.SucceededKeys = GetSuccessKeys(eventStatsResponse.Metadata.FailedKeys, initialEventList)

	return eventStatsResponse, 200
}
