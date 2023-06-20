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
	bingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/oauth2"
)

type Client struct {
	URL    string
	client *http.Client
}
type BingAdsBulkUploader struct {
	destName             string
	service              bingads.BulkServiceI
	destinationIDFileMap map[string]string
	timeout              time.Duration
	logger               logger.Logger
	client               Client
	MaxUploadSize        int64
}

func NewBingAdsBulkUploader(service bingads.BulkServiceI, opts common.AsyncDestinationOpts, client *Client) *BingAdsBulkUploader {
	return &BingAdsBulkUploader{
		destName:      "BING_ADS",
		service:       service,
		timeout:       opts.HttpTimeout,
		logger:        logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("BingAds").Child("BingAdsBulkUploader"),
		client:        *client,
		MaxUploadSize: opts.MaxUploadSize,
	}
}

func CreateZipFile(filePath string, audienceId string, maxUploadSize int64) (string, []int64, []int64, error) {
	failedJobIds := []int64{}
	successJobIds := []int64{}
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	uuid := uuid.New()
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", nil, nil, err
	}
	path := path.Join(tmpDirPath, localTmpDirName, uuid.String())
	csvFilePath := fmt.Sprintf(`%v.csv`, path)
	zipFilePath := fmt.Sprintf(`%v.zip`, path)
	textFile, err := os.Open(filePath)
	if err != nil {
		return "", nil, nil, err
	}
	defer textFile.Close()
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, nil, err
	}
	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write([]string{"Type", "Status", "Id", "Parent Id", "Client Id", "Modified Time", "Name", "Description", "Scope", "Audience", "Action Type", "Sub Type", "Text"})
	csvWriter.Write([]string{"Format Version", "", "", "", "", "", "6.0", "", "", "", "", "", ""})
	csvWriter.Write([]string{"Customer List", "", audienceId, "", "", "", "", "", "", "", "Add", "", ""})
	scanner := bufio.NewScanner(textFile)
	size := 0
	for scanner.Scan() {
		line := scanner.Text()
		var data Data
		err := json.Unmarshal([]byte(line), &data)
		if err != nil {
			return "", nil, nil, err
		}
		marshaledUploadlist, err := json.Marshal(data.Message.List)
		size = size + len([]byte(marshaledUploadlist))
		//if int64(size) < 100*bytesize.MB {
		if int64(size) < maxUploadSize {
			for _, uploadData := range data.Message.List {
				csvWriter.Write([]string{"Customer List Item", "", "", audienceId, uploadData.Email, "", "", "", "", "", "", "Email", uploadData.HashedEmail})
			}
			successJobIds = append(successJobIds, data.Metadata.JobID)
		} else {
			// ?? how to add test case for this
			failedJobIds = append(failedJobIds, data.Metadata.JobID)
		}

	}
	csvWriter.Flush()

	// Create the ZIP file and add the CSV file to it
	zipFile, err := os.Create(zipFilePath)
	if err != nil {
		return "", nil, nil, err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)

	csvFileInZip, err := zipWriter.Create(filepath.Base(csvFilePath))
	if err != nil {
		return "", nil, nil, err
	}

	csvFile.Seek(0, 0)
	_, err = io.Copy(csvFileInZip, csvFile)
	if err != nil {
		return "", nil, nil, err
	}

	// Close the ZIP writer
	err = zipWriter.Close()
	if err != nil {
		return "", nil, nil, err
	}
	// Remove the csv file after creating the zip file
	err = os.Remove(csvFilePath)
	if err != nil {
		return "", nil, nil, err
	}

	return zipFilePath, successJobIds, failedJobIds, nil
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
			os.MkdirAll(path, f.Mode())
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
	typeIndex := -1
	if len(records) > 0 {
		header := records[0]
		for i, column := range header {
			if column == "Client Id" {
				clientIDIndex = i
			} else if column == "Error" {
				errorIndex = i
			} else if column == "Type" {
				typeIndex = i
			}
		}
	}

	// Declare variables for storing data

	clientIDErrors := make(map[string]map[string]struct{})

	// Iterate over the remaining rows and filter based on the 'Type' field containing the substring 'Error'
	// The error messages are present on the rows where the corresponding Type column values are "Customer List Error", "Customer List Item Error" etc
	for _, record := range records[1:] {
		if typeIndex >= 0 && typeIndex < len(record) && strings.Contains(record[typeIndex], "Error") {
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
	List []User `json:"List"`
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
This function create zip file from the text file created by the batchrouter
It takes the text file path as input and returns the zip file path
The maximum size of the zip file is 100MB, if the size of the zip file exceeds 100MB then the job is marked as failed
*/
func (b *BingAdsBulkUploader) Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {

	destConfig := DestinationConfig{}
	jsonConfig, _ := json.Marshal(destination.Config)
	_ = json.Unmarshal(jsonConfig, &destConfig)

	urlResp, err := b.service.GetBulkUploadUrl()

	if err != nil {
		b.logger.Error("Error in getting bulk upload url: %v", err)
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  `{"error" : "unable to get bulk upload url"}`,
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	if urlResp.UploadUrl == "" || urlResp.RequestId == "" {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  `{"error" : "getting empty string in upload url or request id"}`,
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	filePath, successJobIDs, failedJobIds, err := CreateZipFile(asyncDestStruct.FileName, destConfig.AudienceId, b.MaxUploadSize)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while transforming the file. %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	uploadBulkFileResp, errorDuringUpload := b.service.UploadBulkFile(urlResp.UploadUrl, filePath)

	err = os.Remove(filePath)
	if err != nil {
		b.logger.Error("Error in removing zip file: %v", err)
		//To do add an alert here
	}
	if errorDuringUpload != nil {
		b.logger.Error("Error in uploading the bulk file: %v", err)
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  `{"error" : "unable to upload bulk file"}`,
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	if uploadBulkFileResp.RequestId == "" || uploadBulkFileResp.TrackingId == "" {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  `{"error" : "getting empty string in tracking id or request id"}`,
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	//Remove the zip file after uploading it

	// success case
	var parameters common.ImportParameters
	parameters.ImportId = uploadBulkFileResp.RequestId
	importParameters, err := json.Marshal(parameters)
	if err != nil {
		b.logger.Error("Errored in Marshalling parameters" + err.Error())
	}
	return common.AsyncUploadOutput{
		ImportingJobIDs:     successJobIDs,
		FailedJobIDs:        append(asyncDestStruct.FailedJobIDs, failedJobIds...),
		FailedReason:        `{"error":"Jobs flowed over the prescribed limit"}`,
		ImportingParameters: stdjson.RawMessage(importParameters),
		ImportingCount:      len(asyncDestStruct.ImportingJobIDs),
		FailedCount:         len(asyncDestStruct.FailedJobIDs) + len(failedJobIds),
		DestinationID:       destination.ID,
	}
}

func (b *BingAdsBulkUploader) Poll(pollInput common.AsyncPoll) (common.PollStatusResponse, int) {
	requestId := pollInput.ImportId
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
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			panic(fmt.Errorf("error creating output directory: err: %w", err))
		}

		// Download the zip file
		fileLoadResp, err := b.client.client.Get(modifiedUrl)
		if err != nil {
			fmt.Println("Error downloading zip file:", err)
			panic(fmt.Errorf("BRT: Failed creating temporary file. Err: %w", err))
		}
		defer fileLoadResp.Body.Close()

		// Create a temporary file to save the downloaded zip file
		tempFile, err := os.CreateTemp("", fmt.Sprintf("bingads_%s_*.zip", pollInput.ImportId))
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
	} else {
		// this will include authenticaion key errors
		// file will not be available for this case.
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
		return nil, fmt.Errorf("Error occured while generating the accessToken")
	}

	token := &oauth2.Token{
		AccessToken:  secret.AccessToken,
		RefreshToken: secret.RefreshToken,
		Expiry:       time.Now().Add(time.Hour), // Set the token expiry time
	}
	return token, nil
}

func newManagerInternal(destination *backendconfig.DestinationT, oauthClient oauth.Authorizer, opts common.AsyncDestinationOpts) (*BingAdsBulkUploader, error) {
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
	bingads := NewBingAdsBulkUploader(bingads.NewBulkService(session), opts, &clientNew)
	return bingads, nil
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig, opts common.AsyncDestinationOpts) (*BingAdsBulkUploader, error) {
	oauthClient := oauth.NewOAuthErrorHandler(backendConfig)
	return newManagerInternal(destination, oauthClient, opts)
}

func (b *BingAdsBulkUploader) GetUploadStats(UploadStatsInput common.FetchUploadJobStatus) (common.GetUploadStatsResponse, int) {
	// Function implementation
	filePath := UploadStatsInput.OutputFilePath
	// utilImpl := BingAdsUtilsImpl{}
	records := ReadPollResults(filePath)
	status := "200"
	clientIDErrors := ProcessPollStatusData(records)

	// considering importing jobs are the primary list of jobs sent
	// making an array of those jobIds
	importList := UploadStatsInput.ImportingList
	var initialEventList []int64
	for _, job := range importList {
		initialEventList = append(initialEventList, job.JobID)
	}
	// Build the response body
	eventStatsResponse := common.GetUploadStatsResponse{
		Status: status,
		Metadata: common.EventStatMeta{
			FailedKeys:    GetFailedKeys(clientIDErrors),
			ErrFailed:     nil,
			WarningKeys:   []int64{},
			ErrWarning:    nil,
			SucceededKeys: GetSuccessKeys(GetFailedKeys(clientIDErrors), initialEventList),
			ErrSuccess:    nil,
			FailedReasons: GetFailedReasons(clientIDErrors),
		},
	}

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
