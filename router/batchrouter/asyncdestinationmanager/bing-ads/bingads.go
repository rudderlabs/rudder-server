package bingads

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/oauth2"

	bingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
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

func NewBingAdsBulkUploader(name string, service bingads.BulkServiceI, client *Client) *BingAdsBulkUploader {
	return &BingAdsBulkUploader{
		destName: name,
		service:  service,
		logger:   logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("BingAds").Child("BingAdsBulkUploader"),
		client:   *client,
	}
}

func generateClientID(user User, metadata Metadata) string {
	jobId := metadata.JobID
	return strconv.FormatInt(jobId, 10) + "<<>>" + user.HashedEmail
}

func csvZipFileCreator(audienceId string, actionType string) (string, *csv.Writer, string) {
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	uuid := uuid.New()
	tmpDirPath, err := misc.CreateTMPDIR()
	path := path.Join(tmpDirPath, localTmpDirName, uuid.String())
	csvFilePath := fmt.Sprintf(`%v.csv`, path)
	zipFilePath := fmt.Sprintf(`%v.zip`, path)
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, ""
	}
	csvWriter := csv.NewWriter(csvFile)
	err = csvWriter.Write([]string{"Type", "Status", "Id", "Parent Id", "Client Id", "Modified Time", "Name", "Description", "Scope", "Audience", "Action Type", "Sub Type", "Text"})
	err = csvWriter.Write([]string{"Format Version", "", "", "", "", "", "6.0", "", "", "", "", "", ""})
	err = csvWriter.Write([]string{"Customer List", "", audienceId, "", "", "", "", "", "", "", actionType, "", ""})
	return csvFilePath, csvWriter, zipFilePath
}

func convertCsvToZip(csvFilePath string, zipFilePath string, eventCount int) error {
	csvFile, err := os.Open(csvFilePath)
	zipFile, err := os.Create(zipFilePath)
	if eventCount == 0 {
		os.Remove(csvFilePath)
		os.Remove(zipFilePath)
		return nil
	}
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)

	csvFileInZip, err := zipWriter.Create(filepath.Base(csvFilePath))
	if err != nil {
		return err
	}

	_, err = csvFile.Seek(0, 0)
	_, err = io.Copy(csvFileInZip, csvFile)
	if err != nil {
		return err
	}

	// Close the ZIP writer
	err = zipWriter.Close()
	if err != nil {
		return err
	}
	// Remove the csv file after creating the zip file
	err = os.Remove(csvFilePath)
	if err != nil {
		return err
	}
	return nil
}

func populateZipFile(fileSize *int, line string, destName string, eventCount *int, data Data, csvWriter *csv.Writer, audienceId string, successJobIds *[]int64, failedJobIds *[]int64) {
	*fileSize = *fileSize + len([]byte(line))
	if int64(*fileSize) < common.GetBatchRouterConfigInt64("MaxUploadLimit", destName, 100*bytesize.MB) && *eventCount < 4000000 {
		*eventCount += 1
		for _, uploadData := range data.Message.List {
			clientId := generateClientID(uploadData, data.Metadata)
			csvWriter.Write([]string{"Customer List Item", "", "", audienceId, clientId, "", "", "", "", "", "", "Email", uploadData.HashedEmail})
		}
		*successJobIds = append(*successJobIds, data.Metadata.JobID)
	} else {
		*failedJobIds = append(*failedJobIds, data.Metadata.JobID)
	}
}

/*
Depending on add, remove and update action we are creating 3 different zip files using this function
It is also returning the list of succeed and failed events lists.
The following map indicates the index->actionType mapping
0-> Add
1-> Remove
2-> Update
*/
func (b *BingAdsBulkUploader) CreateZipFile(filePath, audienceId string) ([3]string, [3][]int64, [3][]int64, error) {
	failedJobIds := [3][]int64{}
	successJobIds := [3][]int64{}

	textFile, err := os.Open(filePath)
	if err != nil {
		return [3]string{"", "", ""}, successJobIds, failedJobIds, err
	}
	defer textFile.Close()

	if err != nil {
		return [3]string{"", "", ""}, successJobIds, failedJobIds, err
	}
	var csvWriter [3]*csv.Writer
	var csvFilePaths [3]string
	var zipFilePaths [3]string
	var eventCount [3]int
	var actionTypes = [...]string{"Add", "Remove", "Update"}
	for index, actionType := range actionTypes {
		csvFilePaths[index], csvWriter[index], zipFilePaths[index] = csvZipFileCreator(audienceId, actionType)
	}
	scanner := bufio.NewScanner(textFile)
	fileSize := []int{0, 0, 0} //
	for scanner.Scan() {
		line := scanner.Text()
		var data Data
		err := json.Unmarshal([]byte(line), &data)
		if err != nil {
			return [3]string{"", "", ""}, successJobIds, failedJobIds, err
		}

		payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
		payloadSizeStat.Observe(float64(len(data.Message.List)))
		switch data.Message.Action {
		case "Add":
			populateZipFile(&fileSize[0], line, b.destName, &eventCount[0], data, csvWriter[0], audienceId, &successJobIds[0], &failedJobIds[0])
		case "Remove":
			populateZipFile(&fileSize[1], line, b.destName, &eventCount[1], data, csvWriter[1], audienceId, &successJobIds[1], &failedJobIds[1])
		case "Update":
			populateZipFile(&fileSize[2], line, b.destName, &eventCount[2], data, csvWriter[2], audienceId, &successJobIds[2], &failedJobIds[2])
		}

	}
	for index := range actionTypes {
		csvWriter[index].Flush()
		convertCsvToZip(csvFilePaths[index], zipFilePaths[index], eventCount[index])
	}
	// Create the ZIP file and add the CSV file to it

	return zipFilePaths, successJobIds, failedJobIds, nil
}

func Unzip(zipFile, targetDir string) ([]string, error) {
	var filePaths []string

	r, err := zip.OpenReader(zipFile)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for _, f := range r.File {
		// Open each file in the zip archive
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		// Create the corresponding file in the target directory
		path := filepath.Join(targetDir, f.Name)
		if f.FileInfo().IsDir() {
			// Create directories if the file is a directory
			err = os.MkdirAll(path, f.Mode())
		} else {
			// Create the file and copy the contents
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return nil, err
			}
			defer file.Close()

			_, err = io.Copy(file, rc)
			if err != nil {
				return nil, err
			}

			// Append the file path to the list
			filePaths = append(filePaths, path)
		}
	}

	return filePaths, nil
}

// ReadPollResults reads the CSV file and returns the records
// In the below format (only adding relevant keys)
//
//	[][]string{
//		{"Client Id", "Error", "Type"},
//		{"1<<>>client1", "error1", "Customer List Error"},
//		{"1<<>>client2", "error1", "Customer List Item Error"},
//		{"1<<>>client2", "error2", "Customer List Item Error"},
//	}
func ReadPollResults(filePath string) [][]string {
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Error opening the CSV file:", err)
	}
	// defer file.Close() and remove
	defer func() {
		err := file.Close()
		if err != nil {
			log.Fatal("Error closing the CSV file:", err)
		}
		// remove the file after the response has been written
		err = os.Remove(filePath)
		if err != nil {
			panic(err)
		}
	}()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records from the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Error reading CSV:", err)
	}
	return records
}

// This function processes the CSV records and returns the JobIDs and the corresponding error messages
// In the below format:
//
//	map[string]map[string]struct{}{
//		"1": {
//			"error1": {},
//		},
//		"2": {
//			"error1": {},
//			"error2": {},
//		},
//	}
func ProcessPollStatusData(records [][]string) map[string]map[string]struct{} {
	clientIDIndex := -1
	errorIndex := -1
	typeIndex := 0
	if len(records) > 0 {
		header := records[0]
		for i, column := range header {
			fmt.Println(column)
			if column == "Client Id" {
				clientIDIndex = i
			} else if column == "Error" {
				errorIndex = i
			}
		}
	}

	// Declare variables for storing data

	clientIDErrors := make(map[string]map[string]struct{})

	// Iterate over the remaining rows and filter based on the 'Type' field containing the substring 'Error'
	// The error messages are present on the rows where the corresponding Type column values are "Customer List Error", "Customer List Item Error" etc
	for index, record := range records[1:] {
		fmt.Println(index)
		// fmt.Println(record[0])
		rowname := string(record[typeIndex])
		if typeIndex < len(record) && strings.Contains(rowname, "Error") {
			if clientIDIndex >= 0 && clientIDIndex < len(record) {
				// expecting the client ID is present as jobId<<>>clientId
				clientID := strings.Split(record[clientIDIndex], "<<>>")
				if len(clientID) >= 2 {
					errorSet, ok := clientIDErrors[clientID[0]]
					if !ok {
						errorSet = make(map[string]struct{})
						// making the structure as jobId: [error1, error2]
						clientIDErrors[clientID[0]] = errorSet
					}
					errorSet[record[errorIndex]] = struct{}{}

				}
			}
		}
	}
	return clientIDErrors
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
	fmt.Println("Polling Bing Ads")
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
		// the final status file needs to be downloaded
		fileAccessUrl := uploadStatusResp.ResultFileUrl
		modifiedUrl := strings.ReplaceAll(fileAccessUrl, "amp;", "")
		outputDir := "/tmp"
		// Create output directory if it doesn't exist
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			panic(fmt.Errorf("error creating output directory: err: %w", err))
		}

		// Download the zip file
		fileLoadResp, err := http.Get(modifiedUrl)
		if err != nil {
			fmt.Println("Error downloading zip file:", err)
			panic(fmt.Errorf("BRT: Failed creating temporary file. Err: %w", err))
		}
		defer fileLoadResp.Body.Close()

		// Create a temporary file to save the downloaded zip file
		tempFile, err := os.CreateTemp("", fmt.Sprintf("bingads_%s_*.zip", requestId))
		if err != nil {
			panic(fmt.Errorf("BRT: Failed creating temporary file. Err: %w", err))
		}
		defer os.Remove(tempFile.Name())

		// Save the downloaded zip file to the temporary file
		_, err = io.Copy(tempFile, fileLoadResp.Body)
		if err != nil {
			panic(fmt.Errorf("BRT: Failed saving zip file. Err: %w", err))
		}
		// Extract the contents of the zip file to the output directory
		filePaths, err := Unzip(tempFile.Name(), outputDir)
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

func generateArrayOfStrings(value string) []string {
	result := []string{}
	requestIdsArray := strings.Split(value, ",")
	for _, requestId := range requestIdsArray {
		if requestId != "" {
			result = append(result, requestId)
		}
	}
	return result
}

func (b *BingAdsBulkUploader) Poll(pollInput common.AsyncPoll) (common.PollStatusResponse, int) {
	fmt.Println("Polling Bing Ads")
	var cumulativeResp common.PollStatusResponse
	var statusCode int
	requestIdsArray := generateArrayOfStrings(pollInput.ImportId)
	for index, requestId := range requestIdsArray {
		fmt.Println(index)
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

// create array of failed job Ids from clientIDErrors
func GetFailedKeys(clientIDErrors map[string]map[string]struct{}) []int64 {
	keys := make([]int64, 0, len(clientIDErrors))
	for key := range clientIDErrors {
		intKey, _ := strconv.ParseInt(key, 10, 64)
		keys = append(keys, intKey)
	}
	return keys
}

// get the list of unique error messages for a particular jobId.
func GetFailedReasons(clientIDErrors map[string]map[string]struct{}) map[string]string {
	reasons := make(map[string]string)
	for key, errors := range clientIDErrors {
		errorList := make([]string, 0, len(errors))
		for k := range errors {
			errorList = append(errorList, k)
		}
		reasons[key] = strings.Join(errorList, ", ")
	}
	return reasons
}

type tokenSource struct {
	workspaceID     string
	destinationName string
	accountID       string
	oauthClient     oauth.Authorizer
}

func (ts *tokenSource) generateToken() (*secretStruct, error) {
	refreshTokenParams := oauth.RefreshTokenParams{
		WorkspaceId: ts.workspaceID,
		DestDefName: ts.destinationName,
		AccountId:   ts.accountID,
	}
	statusCode, authResponse := ts.oauthClient.FetchToken(&refreshTokenParams)
	if statusCode != 200 {
		return nil, fmt.Errorf("Error in fetching access token")
	}
	secret := secretStruct{}
	err := json.Unmarshal(authResponse.Account.Secret, &secret)
	if err != nil {
		return nil, fmt.Errorf("Error in unmarshalling secret: %v", err)
	}
	currentTime := time.Now()
	expirationTime, err := time.Parse(misc.RFC3339Milli, secret.ExpirationDate)
	if err != nil {
		return nil, fmt.Errorf("Error in parsing expirationDate: %v", err)
	}
	if currentTime.After(expirationTime) {
		refreshTokenParams.Secret = authResponse.Account.Secret
		statusCode, authResponse = ts.oauthClient.RefreshToken(&refreshTokenParams)
		if statusCode != 200 {
			return nil, fmt.Errorf("Error in refreshing access token")
		}
		err = json.Unmarshal(authResponse.Account.Secret, &secret)
		if err != nil {
			return nil, fmt.Errorf("Error in unmarshalling secret: %v", err)
		}
		return &secret, nil
	}
	return &secret, nil
}

func (ts *tokenSource) Token() (*oauth2.Token, error) {
	secret, err := ts.generateToken()
	if err != nil {
		return nil, fmt.Errorf("Error occurred while generating the accessToken")
	}

	token := &oauth2.Token{
		AccessToken:  secret.AccessToken,
		RefreshToken: secret.RefreshToken,
		Expiry:       time.Now().Add(time.Hour), // Set the token expiry time
	}
	return token, nil
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
	//filePaths := strings.Split(UploadStatsInput.OutputFilePath, ",")
	filePaths := generateArrayOfStrings(UploadStatsInput.OutputFilePath)
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

// filtering out failed jobIds from the total array of jobIds
// in order to get jobIds of the successful jobs
func GetSuccessKeys(failedEventList, initialEventList []int64) []int64 {
	successfulEvents := make([]int64, 0)

	lookup := make(map[int64]bool)
	for _, element := range failedEventList {
		lookup[element] = true
	}

	for _, element := range initialEventList {
		if !lookup[element] {
			successfulEvents = append(successfulEvents, element)
		}
	}
	return successfulEvents
}
