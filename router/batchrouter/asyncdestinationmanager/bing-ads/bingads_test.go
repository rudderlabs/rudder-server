package bingads

import (
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	time "time"

	"github.com/golang/mock/gomock"
	bingads_sdk "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/bingads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
)

var destination = backendconfig.DestinationT{
	Name: "BingAds",
}

func TestBingAdsUploadSuccessCase(t *testing.T) {

	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
		TrackingId: "randomTrackingId",
		RequestId:  "randomRequestId",
	}, nil)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        "/Users/shrouti/workspace/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/test-files/uploadData.txt",
	}
	expected := common.AsyncUploadOutput{FailedReason: `{"error":"Jobs flowed over the prescribed limit"}`,
		ImportingJobIDs:     []int64{1, 2, 3, 4},
		FailedJobIDs:        []int64{},
		ImportingParameters: stdjson.RawMessage{},
		ImportingCount:      4,
		FailedCount:         0,
	}

	//making upload function call
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	recieved.ImportingParameters = stdjson.RawMessage{}

	assert.Equal(t, recieved, expected)
}

func TestBingAdsUploadFailedGetBulkUploadUrl(t *testing.T) {
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		FileName:        "/Users/shrouti/workspace/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/test-files/uploadData.txt",
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

	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	ClientI := Client{}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, &ClientI)
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
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        "/Users/shrouti/workspace/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/test-files/uploadData.txt",
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:  []int64{1, 2, 3, 4},
		FailedReason:  "{\"error\" : \"unable to upload bulk file\"}",
		FailedCount:   4,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	assert.Equal(t, recieved, expected)
}

func TestBingAdsPollSuccessCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second.Abs(), &clientI)

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
	clientI := Client{}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, &clientI)

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

	// Create a test server with a custom handler function
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Open the zip file
		zipFilePath := "/Users/shrouti/workspace/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/test-files/statuscheck.zip"
		// Set the appropriate headers for a zip file response
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename='uploadstatus.zip'")
		http.ServeFile(w, r, zipFilePath)
	}))
	defer ts.Close()
	client := ts.Client()
	modifiedURL := ts.URL // Use the test server URL
	clientI := Client{client: client, URL: modifiedURL}

	// bingAdsUtilsImpl.EXPECT().Unzip(gomock.Any(), gomock.Any()).Return([]string{"/path/to/file1.csv"}, nil)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, &clientI)

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
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	templateFilePath := "/Users/shrouti/workspace/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/test-files/uploadstatus.csv" // Path of the source file
	testFilePath := "/Users/shrouti/workspace/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/test-files/test_copy.csv"        // Path of the destination folder

	err := DuplicateFile(templateFilePath, testFilePath)
	if err != nil {
		fmt.Printf("Error duplicating file: %v\n", err)
		return
	}
	fmt.Printf("File %s duplicated to %s\n", templateFilePath, testFilePath)
	clientI := Client{}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, &clientI)

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
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
	clientI := Client{}
	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, &clientI)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{},
		// the aim is to fail the text transformation of the file. That is why sending a csv file instead of a txt file
		FileName: "/Users/shrouti/workspace/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/test-files/uploadstatus.csv",
	}
	expected := common.AsyncUploadOutput{
		FailedJobIDs:  []int64{1, 2, 3},
		FailedReason:  `got error while transforming the file. invalid character 'T' looking for beginning of value`,
		FailedCount:   3,
		DestinationID: destination.ID,
	}
	recieved := bulkUploader.Upload(&destination, &asyncDestination)
	assert.Equal(t, recieved, expected)
}
