package bingads

import (
	"encoding/json"
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

	// bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
)

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
		Expiry:      time.Now().Add(time.Hour),
	}
	return token, nil
}

var destination = backendconfig.DestinationT{
	Name: "BingAds",
}

func TestBingAdsUploadSuccessCase(t *testing.T) {

	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bingAdsUtilImpl.EXPECT().CreateZipFile(gomock.Any(), gomock.Any()).Return("randomZipFile.path", []int64{1, 2}, []int64{3}, nil)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
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
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
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

	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
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
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bingAdsUtilImpl.EXPECT().CreateZipFile(gomock.Any(), gomock.Any()).Return("randomZipFile.path", []int64{1, 2}, []int64{3}, nil)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
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
	bingAdsUtilsImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilsImpl)

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
	bingAdsUtilsImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilsImpl)

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
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	bingAdsUtilsImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bingAdsUtilsImpl.EXPECT().Unzip(gomock.Any(), gomock.Any()).Return([]string{"/path/to/file1.csv"}, nil)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilsImpl)

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
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bingAdsUtilImpl.EXPECT().ReadPollResults(gomock.Any()).Return([][]string{
		{"Client Id", "Error", "Type"},
		{"1<<>>client1", "error1", "Customer List Error"},
		{"1<<>>client2", "error1", "Customer List Item Error"},
		{"1<<>>client2", "error2", "Customer List Item Error"},
	})

	bingAdsUtilImpl.EXPECT().ProcessPollStatusData(gomock.Any()).Return(map[string]map[string]struct{}{
		"1": {
			"error1": {},
		},
		"2": {
			"error1": {},
			"error2": {},
		},
	})

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)

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
	expectedStatus := 200
	recievedResponse, RecievedStatus := bulkUploader.GetUploadStats(UploadStatsInput)
	assert.Equal(t, recievedResponse, expectedResp)
	assert.Equal(t, RecievedStatus, expectedStatus)
}

func TestBingAdsUploadFailedWhileTransformingFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

	bingAdsUtilImpl.EXPECT().CreateZipFile(gomock.Any(), gomock.Any()).Return("", nil, nil, fmt.Errorf("Error in creating zip file"))

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
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
