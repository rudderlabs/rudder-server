package asyncdestinationmanager

import (
	stdjson "encoding/json"
	"fmt"
	"testing"
	time "time"

	"github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/bingads"
	mock_oauth "github.com/rudderlabs/rudder-server/mocks/services/oauth"
	bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/bingads_sdk"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

type MyMockFunction struct {
	mock.Mock
}

// Define the mock implementation of the function
func (m *MyMockFunction) CreateZipFile(input string) string {
	// Define the arguments you expect to receive in the function call
	args := m.Called(input)
	// Return the mocked result and error
	return args.String(0)
}

func (m *MyMockFunction) Unzip(zipFile, targetDir string) ([]string, error) {
	// Define the arguments you expect to receive in the function call
	args := m.Called(zipFile, targetDir)
	// Extract the individual return values from the args
	strSlice := args.Get(0).([]string)
	err := args.Error(1)
	// Return the mocked result and error
	return strSlice, err
}

func TestBingAdsClientCreation(t *testing.T) {
	// mockFn := &MyMockFunction{}
	// mockFn.On("CreateZipFile").Return("42")
	_CreateZipFile := bingads.CreateZipFile
	defer func() {
		bingads.CreateZipFile = _CreateZipFile
	}()
	bingads.CreateZipFile = func(filePath string, failedJobIds *[]int64, successJobIds *[]int64, audienceId string) string {
		return "42"
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(oauthClient, bingAdsService, 10*time.Second)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
		TrackingId: "randomTrackingId",
		RequestId:  "randomRequestId",
	}, nil)

	destinaton := backendconfig.DestinationT{
		Name: "BingAds",
	}
	asyncDestination := common.AsyncDestinationStruct{}
	expected := common.AsyncUploadOutput{FailedReason: `{"error":"Jobs flowed over the prescribed limit"}`,
		ImportingParameters: stdjson.RawMessage{},
		ImportingJobIDs:     []int64{}}
	recieved := bulkUploader.Upload(&destinaton, &asyncDestination)
	recieved.ImportingParameters = stdjson.RawMessage{}

	assert.Equal(t, recieved, expected)
}

func TestBingAdsPoll(t *testing.T) {
	_UnzipFile := bingads.Unzip
	defer func() {
		bingads.Unzip = _UnzipFile
	}()
	bingads.Unzip = func(zipFile, targetDir string) ([]string, error) {
		filePaths := []string{
			"/path/to/file1.csv",
		}
		return filePaths, nil
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(oauthClient, bingAdsService, 10*time.Second)
	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(100),
		RequestStatus:   "Completed",
		ResultFileUrl:   "http://dummyurl.com",
	}, nil)
	pollStruct := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}
	expectedResp := common.AsyncStatusResponse{
		Success:        true,
		StatusCode:     200,
		HasFailed:      false,
		HasWarning:     false,
		FailedJobsURL:  "",
		WarningJobsURL: "",
		OutputFilePath: "",
	}
	expectedStatus := 200
	recievedResponse, RecievedStatus := bulkUploader.Poll(pollStruct)

	assert.Equal(t, recievedResponse, expectedResp)
	assert.Equal(t, RecievedStatus, expectedStatus)
}
