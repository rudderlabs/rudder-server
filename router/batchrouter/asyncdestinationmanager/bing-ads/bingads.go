package bingads

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/bingads_sdk"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/utils"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type BingAdsBulkUploader struct {
	destName       string
	accessToken    string
	developerToken string
	refreshToken   string
	oauthClient    *oauth.OAuthErrResHandler
}

type User struct {
	Email       string `json:"email"`
	HashedEmail string `json:"hashedEmail"`
}
type Message struct {
	UploadList []User `json:"uploadList"`
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
func createZipFile(filePath string, failedJobIds *[]int64, successJobIds *[]int64, audienceId string) string {

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
		marshaledUploadlist, err := json.Marshal(data.Message.UploadList)
		size = size + len([]byte(marshaledUploadlist))
		if size < 104857600 {
			for _, uploadData := range data.Message.UploadList {
				csvWriter.Write([]string{"Customer List Item", "", "", audienceId, uploadData.Email, "", "", "", "", "", "", "Email", uploadData.HashedEmail})
			}
			*successJobIds = append(*successJobIds, data.Metadata.JobID)
		} else {
			*failedJobIds = append(*failedJobIds, data.Metadata.JobID)
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

	return zipFilePath
}

func (b *BingAdsBulkUploader) Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {

	destConfig := DestinationConfig{}
	jsonConfig, err := json.Marshal(destination.Config)
	err = json.Unmarshal(jsonConfig, &destConfig)

	failedJobIds := []int64{}
	successJobIDs := []int64{}

	filePath := createZipFile(asyncDestStruct.FileName, &failedJobIds, &successJobIDs, destConfig.AudienceId)
	sessionConfig := bingads.SessionConfig{
		AccountId:      destConfig.CustomerAccountId,
		CustomerId:     destConfig.CustomerId,
		AccessToken:    b.accessToken,
		HTTPClient:     http.DefaultClient,
		DeveloperToken: b.developerToken,
		AccessTokenGenerator: func() (string, string, error) {

			refreshTokenParams := oauth.RefreshTokenParams{
				WorkspaceId: destination.WorkspaceID,
				DestDefName: destination.Name,
				AccountId:   destConfig.RudderAccountId,
			}
			statusCode, authResponse := b.oauthClient.FetchToken(&refreshTokenParams)
			if statusCode != 200 {
				return "", "", fmt.Errorf("Error in fetching access token")
			}
			secret := secretStruct{}
			err = json.Unmarshal(authResponse.Account.Secret, &secret)
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
				statusCode, authResponse = b.oauthClient.RefreshToken(&refreshTokenParams)
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
		},
	}
	session := bingads.NewSession(sessionConfig)
	service := bingads.NewBulkService(session)
	urlResp, err := service.GetBulkUploadUrl()
	if err != nil {
		if err != nil {
			panic(fmt.Errorf("Error in getting bulk upload url: %v", err))
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

	uploadBulkFileResp, err := service.UploadBulkFile(urlResp.UploadUrl, filePath)
	if err != nil {
		if err != nil {
			panic(fmt.Errorf("Error in uploading file: %v", err))
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

	// Remove the zip file after uploading it
	// err = os.Remove(filePath)
	// if err != nil {
	// 	panic(fmt.Errorf("Error in removing zip file: %v", err))
	// }

	var parameters common.Parameters
	parameters.ImportId = uploadBulkFileResp.RequestId
	importParameters, err := json.Marshal(parameters)
	if err != nil {
		panic("Errored in Marshalling" + err.Error())
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

func (b *BingAdsBulkUploader) Poll(importingJob *jobsdb.JobT, payload []byte, timeout time.Duration) ([]byte, int) {
	resp := common.AsyncStatusResponse{
		Success:        true,
		StatusCode:     200,
		HasFailed:      false,
		HasWarning:     false,
		FailedJobsURL:  "",
		WarningJobsURL: "",
	}

	respBytes, err := stdjson.Marshal(resp)
	if err != nil {
		panic(err)
	}

	return respBytes, 200
}
func (b *BingAdsBulkUploader) FetchFailedEvents(*backendconfig.DestinationT, *utils.DestinationWithSources, []*jobsdb.JobT, *jobsdb.JobT, common.AsyncStatusResponse, time.Duration) ([]byte, int) {
	return nil, 0
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) *BingAdsBulkUploader {

	destConfig := DestinationConfig{}
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		panic(fmt.Errorf("Error in marshalling destination config: %v", err))
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		panic(fmt.Errorf("Error in unmarshalling destination config: %v", err))
	}
	oauthClient := oauth.NewOAuthErrorHandler(backendConfig)

	bingads := &BingAdsBulkUploader{destName: "BING_ADS", oauthClient: oauthClient}
	return bingads
}
