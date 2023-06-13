package asyncdestinationmanager

import (
	stdjson "encoding/json"
	"fmt"
	"testing"
	time "time"

	"github.com/golang/mock/gomock"
	bingads_sdk "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/bingads"
	mock_oauth "github.com/rudderlabs/rudder-server/mocks/services/oauth"
	bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
)

type secretStruct struct {
	AccessToken     string
	RefreshToken    string
	Developer_token string
	ExpirationDate  string
}

type TokenSource struct {
	accessToken     string
	WorkspaceID     string
	DestinationName string
	AccountID       string
	backendconfig   backendconfig.BackendConfig
	oauthClient     *mock_oauth.MockAuthorizer
}

func (ts *TokenSource) generateToken() (string, string, error) {

	refreshTokenParams := oauth.RefreshTokenParams{
		WorkspaceId: ts.WorkspaceID,
		DestDefName: ts.DestinationName,
		AccountId:   ts.AccountID,
	}

	statusCode, authResponse := ts.oauthClient.FetchToken(&refreshTokenParams)
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
		statusCode, authResponse = ts.oauthClient.RefreshToken(&refreshTokenParams)
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

type DestinationConfig struct {
	AudienceId               string   `json:"audienceId"`
	CustomerAccountId        string   `json:"customerAccountId"`
	CustomerId               string   `json:"customerId"`
	OneTrustCookieCategories []string `json:"oneTrustCookieCategories"`
	RudderAccountId          string   `json:"rudderAccountId"`
}

var destination = backendconfig.DestinationT{
	Name: "BingAds",
}

func TestBingAdsUploadSuccessCase(t *testing.T) {
	_CreateZipFile := bingads.CreateZipFile
	defer func() {
		bingads.CreateZipFile = _CreateZipFile
	}()
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64, error) {
		successJobIds := []int64{1, 2}
		failedJobIds := []int64{3}
		return "randomZipFile.path", successJobIds, failedJobIds, nil
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
		TrackingId: "randomTrackingId",
		RequestId:  "randomRequestId",
	}, nil)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		FileName:        "randomFileName.txt",
	}
	expected := common.AsyncUploadOutput{FailedReason: `{"error":"Jobs flowed over the prescribed limit"}`,
		ImportingJobIDs:     []int64{1, 2},
		FailedJobIDs:        []int64{3},
		ImportingParameters: stdjson.RawMessage{},
		ImportingCount:      3,
		FailedCount:         1,
	}

	//making upload function call
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	recieved.ImportingParameters = stdjson.RawMessage{}

	assert.Equal(t, recieved, expected)
}
func TestBingAdsUploadFailedGetBulkUploadUrl(t *testing.T) {
	_CreateZipFile := bingads.CreateZipFile
	defer func() {
		bingads.CreateZipFile = _CreateZipFile
	}()
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64, error) {
		successJobIds := []int64{1, 2}
		failedJobIds := []int64{3}
		return "randomZipFile.path", successJobIds, failedJobIds, nil
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		FileName:        "randomFileName.txt",
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:  []int64{1, 2, 3},
		FailedReason:  `unable to get bulk upload url`,
		FailedCount:   3,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	assert.Equal(t, recieved, expected)
}

func TestBingAdsUploadEmptyGetBulkUploadUrl(t *testing.T) {
	_CreateZipFile := bingads.CreateZipFile
	defer func() {
		bingads.CreateZipFile = _CreateZipFile
	}()
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64, error) {
		successJobIds := []int64{1, 2}
		failedJobIds := []int64{3}
		return "randomZipFile.path", successJobIds, failedJobIds, nil
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "",
		RequestId: "",
	}, nil)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		FileName:        "randomFileName.txt",
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:  []int64{1, 2, 3},
		FailedReason:  `getting empty string in upload url or request id`,
		FailedCount:   3,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	assert.Equal(t, recieved, expected)
}

func TestBingAdsUploadFailedUploadBulkFile(t *testing.T) {
	_CreateZipFile := bingads.CreateZipFile
	defer func() {
		bingads.CreateZipFile = _CreateZipFile
	}()
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64, error) {
		successJobIds := []int64{1, 2}
		failedJobIds := []int64{3}
		return "randomZipFile.path", successJobIds, failedJobIds, nil
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		FileName:        "randomFileName.txt",
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:  []int64{1, 2, 3},
		FailedReason:  `unable to upload bulk file`,
		FailedCount:   3,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	assert.Equal(t, recieved, expected)
}

func TestBingAdsPollSuccessCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(100),
		RequestStatus:   "Completed",
		ResultFileUrl:   "http://dummyurl.com",
	}, nil)
	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}
	expectedResp := common.PollStatusResponse{
		Success:        true,
		StatusCode:     200,
		HasFailed:      false,
		HasWarning:     false,
		FailedJobsURL:  "",
		WarningJobsURL: "",
		OutputFilePath: "",
	}
	expectedStatus := 200
	recievedResponse, RecievedStatus := bulkUploader.Poll(pollInput)

	assert.Equal(t, recievedResponse, expectedResp)
	assert.Equal(t, RecievedStatus, expectedStatus)
}
func TestBingAdsPollFailureCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	// oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(nil, fmt.Errorf("failed to get bulk upload status:"))
	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}
	expectedResp := common.PollStatusResponse{
		Success:        false,
		StatusCode:     400,
		HasFailed:      true,
		HasWarning:     false,
		FailedJobsURL:  "",
		WarningJobsURL: "",
		OutputFilePath: "",
	}
	expectedStatus := 500
	recievedResponse, RecievedStatus := bulkUploader.Poll(pollInput)

	assert.Equal(t, recievedResponse, expectedResp)
	assert.Equal(t, RecievedStatus, expectedStatus)
}

func TestBingAdsPollPartialFailureCase(t *testing.T) {
	_GetPollResult := bingads.Unzip
	defer func() {
		bingads.Unzip = _GetPollResult
	}()
	bingads.Unzip = func(zipFile, targetDir string) ([]string, error) {
		filePaths := []string{
			"/path/to/file1.csv",
		}
		return filePaths, nil
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	// oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(100),
		RequestStatus:   "CompletedWithErrors",
		ResultFileUrl:   "http://dummyurl.com",
	}, nil)
	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}
	expectedResp := common.PollStatusResponse{
		Success:        true,
		StatusCode:     200,
		HasFailed:      true,
		HasWarning:     false,
		FailedJobsURL:  "",
		WarningJobsURL: "",
		OutputFilePath: "/path/to/file1.csv",
	}
	expectedStatus := 200
	recievedResponse, RecievedStatus := bulkUploader.Poll(pollInput)

	assert.Equal(t, recievedResponse, expectedResp)
	assert.Equal(t, RecievedStatus, expectedStatus)
}

func TestBingAdsGetUploadStats(t *testing.T) {
	_ReadCSVFile := bingads.ReadPollResults
	_ProcessStatusPollData := bingads.ProcessPollStatusData
	defer func() {
		bingads.ReadPollResults = _ReadCSVFile
		bingads.ProcessPollStatusData = _ProcessStatusPollData
	}()
	bingads.ReadPollResults = func(filePath string) [][]string {
		sampleData := [][]string{
			{"Client Id", "Error", "Type"},
			{"1<<>>client1", "error1", "Customer List Error"},
			{"1<<>>client2", "error1", "Customer List Item Error"},
			{"1<<>>client2", "error2", "Customer List Item Error"},
		}
		return sampleData
	}
	bingads.ProcessPollStatusData = func(records [][]string) map[string]map[string]struct{} {
		processedResponse := map[string]map[string]struct{}{
			"1": {
				"error1": {},
			},
			"2": {
				"error1": {},
				"error2": {},
			},
		}
		return processedResponse
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	// oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)

	UploadStatsInput := common.FetchUploadJobStatus{
		OutputFilePath: "/path/to/file1.csv",
		ImportingList: []*jobsdb.JobT{
			{
				JobID: 1,
			},
			{
				JobID: 2,
			},
			{
				JobID: 3,
			},
		},
	}
	expectedResp := common.GetUploadStatsResponse{
		Status: "200",
		Metadata: common.EventStatMeta{
			FailedKeys: []int64{1, 2},
			ErrFailed:  nil,
			FailedReasons: map[string]string{
				"1": "error1",
				"2": "error1, error2",
			},
			WarningKeys:   []int64{},
			ErrWarning:    nil,
			SucceededKeys: []int64{3},
			ErrSuccess:    nil,
		},
	}
	// Convert the response to JSON
	// expectedResp, _ := stdjson.Marshal(Response)
	expectedStatus := 200
	recievedResponse, RecievedStatus := bulkUploader.GetUploadStats(UploadStatsInput)
	assert.Equal(t, recievedResponse, expectedResp)
	assert.Equal(t, RecievedStatus, expectedStatus)
}
func TestBingAdsUploadFailedWhileTransformingFile(t *testing.T) {
	_CreateZipFile := bingads.CreateZipFile
	defer func() {
		bingads.CreateZipFile = _CreateZipFile
	}()
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64, error) {
		return "", nil, nil, fmt.Errorf("Error in creating zip file")
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(bingAdsService, 10*time.Second)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		FileName:        "randomFileName.txt",
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:  []int64{1, 2, 3},
		FailedReason:  `got error while transforming the file. Error in creating zip file`,
		FailedCount:   3,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	assert.Equal(t, recieved, expected)
}
