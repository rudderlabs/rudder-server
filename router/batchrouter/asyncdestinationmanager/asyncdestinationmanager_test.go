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

type MockFunction struct{}

// Success scenario
func TestBingAdsUploadSuccessCase(t *testing.T) {
	_CreateZipFile := bingads.CreateZipFile
	defer func() {
		bingads.CreateZipFile = _CreateZipFile
	}()
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64) {
		successJobIds := []int64{1, 2}
		failedJobIds := []int64{3}
		return "randomZipFile.path", successJobIds, failedJobIds
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
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64) {
		successJobIds := []int64{1, 2}
		failedJobIds := []int64{3}
		return "randomZipFile.path", successJobIds, failedJobIds
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(oauthClient, bingAdsService, 10*time.Second)
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
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64) {
		successJobIds := []int64{1, 2}
		failedJobIds := []int64{3}
		return "randomZipFile.path", successJobIds, failedJobIds
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(oauthClient, bingAdsService, 10*time.Second)
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
	bingads.CreateZipFile = func(filePath string, audienceId string) (string, []int64, []int64) {
		successJobIds := []int64{1, 2}
		failedJobIds := []int64{3}
		return "randomZipFile.path", successJobIds, failedJobIds
	}
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

	bulkUploader := bingads.NewBingAdsBulkUploader(oauthClient, bingAdsService, 10*time.Second)
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
