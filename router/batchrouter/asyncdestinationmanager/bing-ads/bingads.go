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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/bingads_sdk"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/oauth2"
)

type BingAdsBulkUploader struct {
	destName             string
	accessToken          string
	developerToken       string
	refreshToken         string
	oauthClient          oauth.Authorizer
	service              bingads.BulkServiceI
	destinationIDFileMap map[string]string
	timeout              time.Duration
	logger               logger.Logger
}

func NewBingAdsBulkUploader(oauthClient oauth.Authorizer, service bingads.BulkServiceI, timeout time.Duration) *BingAdsBulkUploader {
	return &BingAdsBulkUploader{
		destName:    "BING_ADS",
		oauthClient: oauthClient,
		service:     service,
		timeout:     timeout,
		logger:      logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("BingAds").Child("BingAdsBulkUploader"),
	}
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

type Response struct {
	Status   string      `json:"status"`
	Metadata MetadataNew `json:"metadata"`
}

type MetadataNew struct {
	FailedKeys    []int64           `json:"failedKeys"`
	FailedReasons map[string]string `json:"failedReasons"`
	WarningKeys   []string          `json:"warningKeys"`
	SucceededKeys []int64           `json:"succeededKeys"`
}

/*
This function create zip file from the text file created by the batchrouter
It takes the text file path as input and returns the zip file path
The maximum size of the zip file is 100MB, if the size of the zip file exceeds 100MB then the job is marked as failed
*/

var CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64) {
	failedJobIds := []int64{}
	successJobIds := []int64{}
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	uuid := uuid.New()
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf("%v%v", tmpDirPath+localTmpDirName, fmt.Sprintf("%v", uuid.String()))
	csvFilePath := fmt.Sprintf(`%v.csv`, path)
	zipFilePath := fmt.Sprintf(`%v.zip`, path)
	textFile, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer textFile.Close()
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		panic(err)
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
			panic(err)
		}
		marshaledUploadlist, err := json.Marshal(data.Message.List)
		size = size + len([]byte(marshaledUploadlist))
		if size < 104857600 {
			for _, uploadData := range data.Message.List {
				csvWriter.Write([]string{"Customer List Item", "", "", audienceId, uploadData.Email, "", "", "", "", "", "", "Email", uploadData.HashedEmail})
			}
			successJobIds = append(successJobIds, data.Metadata.JobID)
		} else {
			failedJobIds = append(failedJobIds, data.Metadata.JobID)
		}

	}
	csvWriter.Flush()

	// Create the ZIP file and add the CSV file to it
	zipFile, err := os.Create(zipFilePath)
	if err != nil {
		panic(err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)

	csvFileInZip, err := zipWriter.Create(filepath.Base(csvFilePath))
	if err != nil {
		panic(err)
	}

	csvFile.Seek(0, 0)
	_, err = io.Copy(csvFileInZip, csvFile)
	if err != nil {
		panic(err)
	}

	// Close the ZIP writer
	err = zipWriter.Close()
	if err != nil {
		panic(err)
	}
	// Remove the csv file after creating the zip file
	err = os.Remove(csvFilePath)
	if err != nil {
		panic(err)
	}

	return zipFilePath, successJobIds, failedJobIds
}

func (b *BingAdsBulkUploader) Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {

	destConfig := DestinationConfig{}
	jsonConfig, _ := json.Marshal(destination.Config)
	_ = json.Unmarshal(jsonConfig, &destConfig)

	urlResp, err := b.service.GetBulkUploadUrl()

	if err != nil {
		b.logger.Error("Error in getting bulk upload url: %v", err)
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  `unable to get bulk upload url`,
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	if urlResp.UploadUrl == "" || urlResp.RequestId == "" {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  `getting empty string in upload url or request id`,
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	filePath, successJobIDs, failedJobIds := CreateZipFile(asyncDestStruct.FileName, destConfig.AudienceId)

	uploadBulkFileResp, err := b.service.UploadBulkFile(urlResp.UploadUrl, filePath)

	if err != nil {
		b.logger.Error("Error in uploading the bulk file: %v", err)
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  `unable to upload bulk file`,
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	if uploadBulkFileResp.RequestId == "" || uploadBulkFileResp.TrackingId == "" {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  `getting empty string in tracking id or request id`,
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	//Remove the zip file after uploading it
	err = os.Remove(filePath)
	if err != nil {
		b.logger.Error("Error in removing zip file: %v", err)
		//To do add an alert here
	}

	// success case
	var parameters common.Parameters
	parameters.ImportId = uploadBulkFileResp.RequestId
	// parameters.PollUrl = pollUrl
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

var Unzip = func(zipFile, targetDir string) ([]string, error) {
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

// This function unzips the poll status file downloaded
// func Unzip(zipFile, targetDir string) ([]string, error) {
// 	var filePaths []string

// 	r, err := zip.OpenReader(zipFile)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer r.Close()

// 	for _, f := range r.File {
// 		// Open each file in the zip archive
// 		rc, err := f.Open()
// 		if err != nil {
// 			return nil, err
// 		}
// 		defer rc.Close()

// 		// Create the corresponding file in the target directory
// 		path := filepath.Join(targetDir, f.Name)
// 		if f.FileInfo().IsDir() {
// 			// Create directories if the file is a directory
// 			os.MkdirAll(path, f.Mode())
// 		} else {
// 			// Create the file and copy the contents
// 			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
// 			if err != nil {
// 				return nil, err
// 			}
// 			defer file.Close()

// 			_, err = io.Copy(file, rc)
// 			if err != nil {
// 				return nil, err
// 			}

// 			// Append the file path to the list
// 			filePaths = append(filePaths, path)
// 		}
// 	}

// 	return filePaths, nil
// }

func (b *BingAdsBulkUploader) Poll(pollStruct common.AsyncPoll) (common.AsyncStatusResponse, int) {
	requestId := pollStruct.ImportId
	uploadStatusResp, err := b.service.GetBulkUploadStatus(requestId)
	if err != nil {
		panic("BRT: Failed to poll status for bingAds" + err.Error())
	}
	var resp common.AsyncStatusResponse
	var statusCode int
	var allSuccessPercentage int = 100
	if uploadStatusResp.PercentComplete == int64(allSuccessPercentage) && uploadStatusResp.RequestStatus == "Completed" {
		// all successful events, do not need to download the file.
		resp = common.AsyncStatusResponse{
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
		fileLoadResp, err := http.Get(modifiedUrl)
		if err != nil {
			fmt.Println("Error downloading zip file:", err)
			panic(fmt.Errorf("BRT: Failed creating temporary file. Err: %w", err))
		}
		defer fileLoadResp.Body.Close()

		// Create a temporary file to save the downloaded zip file
		tempFile, err := os.CreateTemp("", "downloaded_zip_*.zip")
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
			panic(fmt.Errorf("BRT: Failed saving zip file extracting zip file Err: %w", err))
		}

		// extracting file paths
		var outputPath string
		for _, filePath := range filePaths {
			outputPath = filePath
		}
		resp = common.AsyncStatusResponse{
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
		resp = common.AsyncStatusResponse{
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
func getFailedKeys(clientIDErrors map[string]map[string]struct{}) []int64 {
	keys := make([]int64, 0, len(clientIDErrors))
	for key := range clientIDErrors {
		intKey, _ := strconv.ParseInt(key, 10, 64)
		keys = append(keys, intKey)
	}
	return keys
}

// get the list of unique error messages for a particular jobId.
func getFailedReasons(clientIDErrors map[string]map[string]struct{}) map[string]string {
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

// filtering out failed jobIds from the total array of jobIds
// in order to get jobIds of the successful jobs
func getSuccessKeys(failedEventList, initialEventList []int64) []int64 {
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

func (b *BingAdsBulkUploader) FetchFailedEvents(failedJobsStatus common.FetchFailedStatus) ([]byte, int) {
	filePath := failedJobsStatus.OutputFilePath
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Error opening the CSV file:", err)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records from the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Error reading CSV:", err)
	}

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
	status := "200"

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

	// considering importing jobs are the primary list of jobs sent
	// making an array of those jobIds
	importList := failedJobsStatus.ImportingList
	var initialEventList []int64
	for _, job := range importList {
		initialEventList = append(initialEventList, job.JobID)
	}

	// Build the response struct
	response := Response{
		Status: status,
		Metadata: MetadataNew{
			FailedKeys:    getFailedKeys(clientIDErrors),
			FailedReasons: getFailedReasons(clientIDErrors),
			WarningKeys:   []string{},
			SucceededKeys: getSuccessKeys(getFailedKeys(clientIDErrors), initialEventList),
		},
	}

	// Convert the response to JSON
	respBytes, err := stdjson.Marshal(response)
	if err != nil {
		log.Fatal("Error converting to JSON:", err)
	}

	// remove the file after the response has been written
	err = os.Remove(filePath)
	if err != nil {
		panic(err)
	}
	return respBytes, 200
}

// retrieves jobIds from metadata based on requirements, eg: successKeys, warningKeys, failedKeys
func (b *BingAdsBulkUploader) RetrieveImportantKeys(metadata map[string]interface{}, retrieveKeys string) ([]int64, error) {
	retrievedKeys, ok := metadata[retrieveKeys].([]interface{})
	if !ok {
		panic("failedKeys is not an array")
	}
	retrievedKeysArr := make([]int64, len(retrievedKeys))
	for index, value := range retrievedKeys {
		retrievedKeysArr[index] = int64(value.(float64))
	}
	return retrievedKeysArr, nil
}

type TokenSource struct {
	accessToken     string
	WorkspaceID     string
	DestinationName string
	AccountID       string
	backendconfig   backendconfig.BackendConfig
}

func (ts *TokenSource) generateToken() (string, string, error) {

	refreshTokenParams := oauth.RefreshTokenParams{
		WorkspaceId: ts.WorkspaceID,
		DestDefName: ts.DestinationName,
		AccountId:   ts.AccountID,
	}
	oauthClient := oauth.NewOAuthErrorHandler(ts.backendconfig)
	statusCode, authResponse := oauthClient.FetchToken(&refreshTokenParams)
	if statusCode != 200 {
		return "", "", fmt.Errorf("Error in fetching access token")
	}
	secret := secretStruct{}
	err := json.Unmarshal(authResponse.Account.Secret, &secret)
	if err != nil {
		return "", "", fmt.Errorf("Error in unmarshalling secret: %v", err)
	}
	currentTime := time.Now()
	expirationTime, err := time.Parse(misc.RFC3339Milli, secret.ExpirationDate)
	if err != nil {
		return "", "", fmt.Errorf("Error in parsing expirationDate: %v", err)
	}
	if currentTime.After(expirationTime) {
		refreshTokenParams.Secret = authResponse.Account.Secret
		statusCode, authResponse = oauthClient.RefreshToken(&refreshTokenParams)
		if statusCode != 200 {
			return "", "", fmt.Errorf("Error in refreshing access token")
		}
		err = json.Unmarshal(authResponse.Account.Secret, &secret)
		if err != nil {
			return "", "", fmt.Errorf("Error in unmarshalling secret: %v", err)
		}
		return secret.AccessToken, secret.Developer_token, nil
	}
	return secret.AccessToken, secret.Developer_token, nil

}
func (ts *TokenSource) Token() (*oauth2.Token, error) {
	accessToken, _, err := ts.generateToken()
	if err != nil {
		return nil, fmt.Errorf("Error occured while generating the accessToken")
	}
	ts.accessToken = accessToken

	token := &oauth2.Token{
		AccessToken: ts.accessToken,
		Expiry:      time.Now().Add(time.Hour), // Set the token expiry time
	}
	return token, nil
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig, HTTPTimeout time.Duration) *BingAdsBulkUploader {

	destConfig := DestinationConfig{}
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		panic(fmt.Errorf("Error in marshalling destination config: %v", err))
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		panic(fmt.Errorf("Error in unmarshalling destination config: %v", err))
	}

	tokenSource := TokenSource{
		WorkspaceID:     destination.WorkspaceID,
		DestinationName: destination.Name,
		AccountID:       destConfig.RudderAccountId,
		backendconfig:   backendConfig,
	}
	_, developerToken, _ := tokenSource.generateToken()
	oauthClient := oauth.NewOAuthErrorHandler(backendConfig)
	sessionConfig := bingads.SessionConfig{
		DeveloperToken: developerToken,
		AccountId:      destConfig.CustomerAccountId,
		CustomerId:     destConfig.CustomerId,
		HTTPClient:     http.DefaultClient,
		TokenSource:    &tokenSource,
	}
	session := bingads.NewSession(sessionConfig)

	bingads := &BingAdsBulkUploader{destName: "BING_ADS", oauthClient: oauthClient, service: bingads.NewBulkService(session), timeout: HTTPTimeout}
	return bingads
}
