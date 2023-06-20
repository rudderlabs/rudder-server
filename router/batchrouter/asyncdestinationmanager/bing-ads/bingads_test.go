package bingads

import (
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	time "time"

	"github.com/golang/mock/gomock"
	bingads_sdk "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	bytesize "github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/bingads"
	mocks_oauth "github.com/rudderlabs/rudder-server/mocks/services/oauth"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var once sync.Once
var destination = backendconfig.DestinationT{
	Name: "BingAds",
	Config: map[string]interface{}{
		"audienceId":        "audience_id",
		"customerAccountId": "customer_account_id",
		"customerId":        "customer_id",
		"rudderAccountId":   "rudder_account_id",
	},
	WorkspaceID: "workspace_id",
}

var currentDir, _ = os.Getwd()

func initBingads() {
	once.Do(func() {
		config.Reset()
		logger.Reset()
		misc.Init()
	})
}

func TestBingAdsUploadPartialSuccessCase(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 1 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
		TrackingId: "randomTrackingId",
		RequestId:  "randomRequestId",
	}, nil)

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	if err != nil {
		fmt.Printf("Failed to create temporary directory: %v\n", err)
		return
	}

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		fmt.Printf("Failed to create the directory 'something': %v\n", err)
		return
	}

	require.NoError(t, err)
	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
	}
	expected := common.AsyncUploadOutput{FailedReason: `{"error":"Jobs flowed over the prescribed limit"}`,
		ImportingJobIDs:     []int64{1, 2},
		FailedJobIDs:        []int64{3, 4},
		ImportingParameters: stdjson.RawMessage{},
		ImportingCount:      4,
		FailedCount:         2,
	}

	//making upload function call
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	recieved.ImportingParameters = stdjson.RawMessage{}

	// Remove the directory and its contents
	err = os.RemoveAll(dir)
	if err != nil {
		fmt.Printf("Failed to remove the temporary directory: %v\n", err)
		return
	}

	assert.Equal(t, recieved, expected)
}

func TestBingAdsUploadFailedGetBulkUploadUrl(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:   []int64{1, 2, 3},
		FailedReason:   "{\"error\" : \"unable to get bulk upload url\"}",
		ImportingCount: 0,
		FailedCount:    3,
		AbortCount:     0,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	assert.Equal(t, recieved, expected)
}

func TestBingAdsUploadEmptyGetBulkUploadUrl(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	ClientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &ClientI)
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
		FailedReason:  "{\"error\" : \"getting empty string in upload url or request id\"}",
		FailedCount:   3,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	assert.Equal(t, recieved, expected)
}

func TestBingAdsUploadFailedUploadBulkFile(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	if err != nil {
		fmt.Printf("Failed to create temporary directory: %v\n", err)
		return
	}

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		fmt.Printf("Failed to create the directory 'something': %v\n", err)
		return
	}

	require.NoError(t, err)
	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:  []int64{1, 2, 3, 4},
		FailedReason:  "{\"error\" : \"unable to upload bulk file\"}",
		FailedCount:   4,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)

	// Remove the directory and its contents
	err = os.RemoveAll(dir)
	if err != nil {
		fmt.Printf("Failed to remove the temporary directory: %v\n", err)
		return
	}
	assert.Equal(t, recieved, expected)
}

func TestBingAdsPollSuccessCase(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)

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
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)

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
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	// Create a test server with a custom handler function
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Open the zip file
		zipFilePath := filepath.Join(currentDir, "test-files/statuscheck.zip")
		// Set the appropriate headers for a zip file response
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename='uploadstatus.zip'")
		http.ServeFile(w, r, zipFilePath)
	}))
	defer ts.Close()
	client := ts.Client()
	modifiedURL := ts.URL // Use the test server URL
	clientI := Client{client: client, URL: modifiedURL}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(100),
		RequestStatus:   "CompletedWithErrors",
		ResultFileUrl:   ts.URL,
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
		OutputFilePath: "/tmp/BulkUpload-05-31-2023-6326c4f9-0745-4c43-8126-621b4a1849ad-Results.csv",
	}
	expectedStatus := 200
	recievedResponse, RecievedStatus := bulkUploader.Poll(pollInput)

	os.Remove(expectedResp.OutputFilePath)

	assert.Equal(t, recievedResponse, expectedResp)
	assert.Equal(t, RecievedStatus, expectedStatus)
}

func TestBingAdsPollPartialFailureCaseWithWrongFilePath(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	// Create a test server with a custom handler function
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Open the zip file
		zipFilePath := filepath.Join(currentDir, "test-files/uploadData.txt")
		// Set the appropriate headers for a zip file response
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename='uploadstatus.zip'")
		http.ServeFile(w, r, zipFilePath)
	}))
	defer ts.Close()
	client := ts.Client()
	modifiedURL := ts.URL // Use the test server URL
	clientI := Client{client: client, URL: modifiedURL}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(100),
		RequestStatus:   "CompletedWithErrors",
		ResultFileUrl:   ts.URL,
	}, nil)
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
	expectedStatus := 400
	recievedResponse, RecievedStatus := bulkUploader.Poll(pollInput)

	os.Remove(expectedResp.OutputFilePath)

	assert.Equal(t, recievedResponse, expectedResp)
	assert.Equal(t, RecievedStatus, expectedStatus)
}

func DuplicateFile(sourcePath, destinationPath string) error {
	// Open the source file
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer sourceFile.Close()

	// Create the destination file
	destinationFile, err := os.Create(destinationPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %v", err)
	}
	defer destinationFile.Close()

	// Copy the contents of the source file to the destination file
	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file contents: %v", err)
	}

	return nil
}

func TestBingAdsGetUploadStats(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	templateFilePath := filepath.Join(currentDir, "test-files/uploadstatus.csv") // Path of the source file
	testFilePath := filepath.Join(currentDir, "test-files/test_copy.csv")        // Path of the destination folder

	err := DuplicateFile(templateFilePath, testFilePath)
	if err != nil {
		fmt.Printf("Error duplicating file: %v\n", err)
		return
	}
	fmt.Printf("File %s duplicated to %s\n", templateFilePath, testFilePath)
	clientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)

	UploadStatsInput := common.FetchUploadJobStatus{
		OutputFilePath: testFilePath,
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
				"1": "error1, error2",
				"2": "error2",
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
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	if err != nil {
		fmt.Printf("Failed to create temporary directory: %v\n", err)
		return
	}

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		fmt.Printf("Failed to create the directory 'something': %v\n", err)
		return
	}

	require.NoError(t, err)
	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		// the aim is to fail the text transformation of the file. That is why sending a csv file instead of a txt file
		FileName: filepath.Join(currentDir, "test-files/uploadstatus.csv"),
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:  []int64{1, 2, 3},
		FailedReason:  `got error while transforming the file. invalid character 'T' looking for beginning of value`,
		FailedCount:   3,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)

	// Remove the directory and its contents
	err = os.RemoveAll(dir)
	if err != nil {
		fmt.Printf("Failed to remove the temporary directory: %v\n", err)
		return
	}
	assert.Equal(t, recieved, expected)
}

func TestNewManagerInternal(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	oauthService := mocks_oauth.NewMockAuthorizer(ctrl)
	oauthService.EXPECT().FetchToken(gomock.Any()).Return(200, &oauth.AuthResponse{
		Account: oauth.AccountSecret{
			ExpirationDate: "",
			Secret: []byte(`
			{
			"AccessToken": "dummyacesstoken",
			"RefreshToken": "dummyRefreshToken",
			"Developer_token": "dummyDeveloperToken",
			"ExpirationDate": "2023-01-31T23:59:59.999Z"
			}`),
		},
	})
	oauthService.EXPECT().RefreshToken(gomock.Any()).Return(200, &oauth.AuthResponse{
		Account: oauth.AccountSecret{
			ExpirationDate: "",
			Secret: []byte(`
			{
			"AccessToken": "dummyacesstoken",
			"RefreshToken": "dummyRefreshToken",
			"Developer_token": "dummyDeveloperToken",
			"ExpirationDate": "2023-01-31T23:59:59.999Z"
			}`),
		},
	})

	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}

	bingAdsUploader, err := newManagerInternal(&destination, oauthService, optsI)
	assert.Nil(t, err)
	assert.NotNil(t, bingAdsUploader)

}

func TestBingAdsUploadNoTrackingId(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	optsI := common.AsyncDestinationOpts{
		MaxUploadSize: 10 * bytesize.KB,
		HttpTimeout:   10 * time.Second,
	}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, optsI, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
		TrackingId: "",
		RequestId:  "",
	}, nil)

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	if err != nil {
		fmt.Printf("Failed to create temporary directory: %v\n", err)
		return
	}

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		fmt.Printf("Failed to create the directory 'something': %v\n", err)
		return
	}

	require.NoError(t, err)
	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
	}
	expected := common.AsyncUploadOutput{FailedReason: `{"error" : "getting empty string in tracking id or request id"}`,
		FailedJobIDs:        []int64{1, 2, 3, 4},
		ImportingParameters: stdjson.RawMessage{},
		ImportingCount:      0,
		FailedCount:         4,
	}

	//making upload function call
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	recieved.ImportingParameters = stdjson.RawMessage{}

	// Remove the directory and its contents
	err = os.RemoveAll(dir)
	if err != nil {
		fmt.Printf("Failed to remove the temporary directory: %v\n", err)
		return
	}

	assert.Equal(t, recieved, expected)
}
