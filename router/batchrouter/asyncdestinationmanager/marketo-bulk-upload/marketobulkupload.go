package marketobulkupload

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/samber/lo"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type MarketoBulkUploader struct {
	destName          string
	destinationConfig MarketoConfig
	logger            logger.Logger
	statsFactory      stats.Stats
	timeout           time.Duration
	csvHeaders        []string
	dataHashToJobId   map[string]int64
	accessToken       MarketoAccessToken
}

type marketoPollInputStruct struct {
	ImportId   string                 `json:"importId"`
	DestType   string                 `json:"destType"`
	DestConfig map[string]interface{} `json:"config"`
}

type MarketoAsyncFailedInput struct {
	Message  map[string]interface{}
	Metadata struct {
		JobID int64
	}
}

type MarketoAsyncFailedPayload struct {
	Config   map[string]interface{}
	Input    []MarketoAsyncFailedInput
	DestType string
	ImportId string
	MetaData common.MetaDataT
}

type MarketoAccessToken struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

const MARKETO_WARNING_HEADER = "Import Warning Reason"
const MARKETO_FAILED_HEADER = "Import Failure Reason"

func getImportingParameters(importID string) json.RawMessage {
	return json.RawMessage(`{"importId": "` + importID + `"}`)
}

func (b *MarketoBulkUploader) getAccessToken() error {
	clientID := b.destinationConfig.ClientId
	clientSecret := b.destinationConfig.ClientSecret
	munchkinId := b.destinationConfig.MunchkinId

	// If the access token is nil or about to expire in 1 min, get a new one
	if b.accessToken.AccessToken == "" || b.accessToken.ExpiresIn < 60 {

		accessTokenURL := fmt.Sprintf("https://%s.mktorest.com/identity/oauth/token?client_id=%s&client_secret=%s&grant_type=client_credentials", munchkinId, clientID, clientSecret)
		req, err := http.NewRequest("POST", accessTokenURL, nil)
		if err != nil {
			return err
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var accessToken MarketoAccessToken
		err = json.Unmarshal(body, &accessToken)
		if err != nil {
			return err
		}

		b.accessToken = accessToken
	}

	return nil

}

func (b *MarketoBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {

	importId := pollInput.ImportId

	munchkinId := b.destinationConfig.MunchkinId
	// accessToken := b.destinationConfig["accessToken"].(string)

	// Construct the API URL
	apiURL := fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads/batch/%s.json", munchkinId, importId)

	// Make the API request
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return common.PollStatusResponse{StatusCode: 500, Error: err.Error(), Complete: false}
	}
	req.Header.Add("Authorization", "Bearer "+b.accessToken.AccessToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return common.PollStatusResponse{StatusCode: 500, Error: err.Error(), Complete: false}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return common.PollStatusResponse{StatusCode: 500, Error: err.Error(), Complete: false}
	}

	var marketoResponse MarketoResponse
	err = json.Unmarshal(body, &marketoResponse)
	if err != nil {
		return common.PollStatusResponse{StatusCode: 500, Error: err.Error(), Complete: false}
	}

	b.logger.Debugf("[Async Destination Manager] Marketo Poll Response: %v", marketoResponse)

	statusCode, category, errorMessage := parseMarketoResponse(marketoResponse)

	if category == "RefreshToken" {
		b.getAccessToken()
		return common.PollStatusResponse{StatusCode: 500, Error: errorMessage, Complete: false}
	}

	switch statusCode {
	case 500:
		return common.PollStatusResponse{StatusCode: int(statusCode), Error: errorMessage, Complete: false}
	case 400:
		return common.PollStatusResponse{StatusCode: int(statusCode), Error: errorMessage, Complete: false}
	case 429:
		return common.PollStatusResponse{StatusCode: int(statusCode), Error: errorMessage, Complete: false}
	}

	if !marketoResponse.Success {
		return common.PollStatusResponse{
			StatusCode: 500,
			Complete:   false,
			HasFailed:  true,
			Error:      "API request was not successful",
		}

	}

	if len(marketoResponse.Result) == 0 {
		return common.PollStatusResponse{
			StatusCode: 500,
			Complete:   false,
			HasFailed:  true,
			Error:      "No result found in the API response",
		}
	}

	batchStatus := marketoResponse.Result[0].Status

	hasFailed := marketoResponse.Result[0].NumOfRowsFailed > 0
	hasWarning := marketoResponse.Result[0].NumOfRowsWithWarning > 0

	coreURl := fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads/batch/%s", b.destinationConfig.MunchkinId, importId)
	failedJobURLs := fmt.Sprintf("%s/failures.json", coreURl)
	warningJobURLs := fmt.Sprintf("%s/warnings.json", coreURl)

	pollStatus := common.PollStatusResponse{}

	switch batchStatus {
	case "Complete":
		pollStatus.Complete = true
		pollStatus.StatusCode = 200
		pollStatus.FailedJobURLs = failedJobURLs
		pollStatus.WarningJobURLs = warningJobURLs
		pollStatus.HasFailed = hasFailed
		pollStatus.HasWarning = hasWarning

	case "Importing", "Queued":
		pollStatus.InProgress = true
		pollStatus.StatusCode = 500
	case "Failed":
		pollStatus.HasFailed = true
		pollStatus.StatusCode = 500
		pollStatus.FailedJobURLs = failedJobURLs
		pollStatus.WarningJobURLs = warningJobURLs
		pollStatus.HasWarning = hasWarning
		pollStatus.Complete = false
		pollStatus.Error = fmt.Sprintf("Marketo Bulk Upload Failed: %s", marketoResponse.Result[0].Message)
	default:
		pollStatus.StatusCode = 500
		pollStatus.Complete = false
		pollStatus.Error = fmt.Sprintf("Unknown status: %s", batchStatus)
	}

	return pollStatus
}

func (b *MarketoBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	// Extract importId from parameters
	var params struct {
		ImportId string `json:"importId"`
	}
	err := json.Unmarshal(input.Parameters, &params)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: 500,
			Error:      "Failed to parse parameters: " + err.Error(),
		}
	}

	// Fetch and parse failed jobs
	failedJobs, err := b.fetchAndParseJobData(input.FailedJobURLs)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: 500,
			Error:      "Failed to fetch failed jobs: " + err.Error(),
		}
	}

	// Fetch and parse warning jobs
	warningJobs, err := b.fetchAndParseJobData(input.WarningJobURLs)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: 500,
			Error:      "Failed to fetch warning jobs: " + err.Error(),
		}
	}

	metadata := b.updateJobStatus(input.ImportingList, failedJobs, warningJobs)

	return common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata:   metadata,
	}
}

func (b *MarketoBulkUploader) updateJobStatus(importingList []*jobsdb.JobT, failedJobs, warningJobs []map[string]string) common.EventStatMeta {

	metadata := common.EventStatMeta{
		FailedKeys:     make([]int64, 0),
		WarningKeys:    make([]int64, 0),
		SucceededKeys:  make([]int64, 0),
		FailedReasons:  make(map[int64]string),
		WarningReasons: make(map[int64]string),
	}

	for _, failedJob := range failedJobs {
		// get failedJob data
		var failedJobRow []string
		for _, col := range b.csvHeaders {
			if val, ok := failedJob[col]; ok {
				failedJobRow = append(failedJobRow, val)
			} else {
				failedJobRow = append(failedJobRow, "")
			}
		}
		// get jobID from jobToDataHash
		hash := calculateHashCode(failedJobRow)
		failedJobId := b.dataHashToJobId[hash]
		if failedJobId != 0 {
			metadata.FailedKeys = append(metadata.FailedKeys, failedJobId)
		}
		failedJobReason := failedJob[MARKETO_FAILED_HEADER]
		if failedJobReason != "" {
			metadata.FailedReasons[failedJobId] = failedJobReason
		}

	}

	for _, warningJob := range warningJobs {
		// get warningJob data
		var warningJobRow []string
		for _, col := range b.csvHeaders {
			if val, ok := warningJob[col]; ok {
				warningJobRow = append(warningJobRow, val)
			} else {
				warningJobRow = append(warningJobRow, "")
			}
		}
		// get jobID from jobToDataHash
		hash := calculateHashCode(warningJobRow)
		warningJobId := b.dataHashToJobId[hash]
		if warningJobId != 0 {
			metadata.WarningKeys = append(metadata.WarningKeys, warningJobId)
		}
		warningJobReason := warningJob[MARKETO_WARNING_HEADER]
		if warningJobReason != "" {
			metadata.WarningReasons[warningJobId] = warningJobReason
		}
	}

	// calculate succeeded keys
	for _, job := range importingList {
		if !lo.Contains(metadata.FailedKeys, job.JobID) && !lo.Contains(metadata.WarningKeys, job.JobID) {
			metadata.SucceededKeys = append(metadata.SucceededKeys, job.JobID)
		}
	}

	return metadata
}

func (b *MarketoBulkUploader) fetchAndParseJobData(url string) ([]map[string]string, error) {
	// Fetch data from URL
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// TODO: Handle marketo response here

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Parse CSV-like response
	reader := csv.NewReader(strings.NewReader(string(body)))

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV: %v", err)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("CSV is empty")
	}

	// The first row is the header
	header := rows[0]

	records := make([]map[string]string, 0, len(rows)-1)

	for _, row := range rows[1:] {
		record := make(map[string]string)
		for i, value := range row {
			if i < len(header) {
				record[header[i]] = value
			}
		}
		records = append(records, record)
	}

	return records, nil
}

func (*MarketoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

func (b *MarketoBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	destConfig := destination.Config
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs
	leadUploadEndpoint := "/bulk/v1/leads/batch"

	uploadURL := fmt.Sprintf("https://%s.mktorest.com%s", destConfig["munchkinId"], leadUploadEndpoint)
	accessToken := destConfig["accessToken"].(string)

	file, err := os.Open(filePath)
	if err != nil {
		panic("BRT: Read File Failed" + err.Error())
	}
	defer file.Close()
	var input []common.AsyncJob
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tempJob common.AsyncJob
		jobBytes := scanner.Bytes()
		err := json.Unmarshal(jobBytes, &tempJob)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  "BRT: Error in Unmarshalling Job: " + err.Error(),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}
		input = append(input, tempJob)
	}

	uploadTimeStat := b.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat := b.statsFactory.NewTaggedStat("payload_size", stats.HistogramType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})
	csvFilePath, headerRowOrder, insertedJobIDs, overflowedJobIDs, err := createCSVFile(destinationID, input, b.dataHashToJobId)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Creating CSV File: " + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	b.csvHeaders = headerRowOrder
	importingJobIDs = insertedJobIDs
	failedJobIDs = append(failedJobIDs, overflowedJobIDs...)

	defer os.Remove(csvFilePath) // Clean up the temporary file

	// Check file size
	fileInfo, err := os.Stat(csvFilePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Getting File Info: " + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	payloadSizeStat.Gauge(float64(fileInfo.Size()))
	startTime := time.Now()

	resp, err := sendHTTPRequest(uploadURL, csvFilePath, accessToken)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Reading Response Body: " + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	defer resp.Body.Close()

	statusCodeHTTP := resp.StatusCode
	if statusCodeHTTP != 200 {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Uploading File: " + resp.Status,
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Reading Response Body: " + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	b.logger.Debugf("[Async Destination Manager] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)

	// parse the response body to MarketoResponse

	var marketoResponse MarketoResponse

	statusCode, category, errorMessage := parseMarketoResponse(marketoResponse)

	if category == "RefreshToken" {
		b.getAccessToken()
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Uploading File: Token Expired " + errorMessage,
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	switch statusCode {
	case 500:
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Uploading File: " + errorMessage,
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	case 400:
		return common.AsyncUploadOutput{
			AbortJobIDs:   append(failedJobIDs, importingJobIDs...),
			AbortReason:   "BRT: Error in Uploading File: " + errorMessage,
			AbortCount:    len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	case 429:
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Uploading File: " + errorMessage,
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	err = json.Unmarshal(responseBody, &marketoResponse)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Error in Unmarshalling Response Body: " + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	// get the import id to fetch details later
	importID := marketoResponse.Result[0].ImportID

	// return the response
	return common.AsyncUploadOutput{
		ImportingJobIDs:     importingJobIDs,
		ImportingParameters: getImportingParameters(importID),
		FailedJobIDs:        failedJobIDs,
		ImportingCount:      len(importingJobIDs),
		FailedCount:         len(failedJobIDs),
		DestinationID:       destinationID,
	}
}

func (b *MarketoBulkUploader) clearHashToJobId() {
	for k := range b.dataHashToJobId {
		delete(b.dataHashToJobId, k)
	}
}
