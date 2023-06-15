package bingads

import (
	"fmt"
	"io"
	"os"
	"testing"
	time "time"

	"github.com/golang/mock/gomock"
	bingads_sdk "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/bingads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/stretchr/testify/assert"
)

var destination = backendconfig.DestinationT{
	Name: "BingAds",
}

// func TestBingAdsUploadSuccessCase(t *testing.T) {

// 	ctrl := gomock.NewController(t)
// 	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
// 	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

// 	bingAdsUtilImpl.EXPECT().CreateZipFile(gomock.Any(), gomock.Any()).Return("randomZipFile.path", []int64{1, 2}, []int64{3}, nil)

// 	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
// 	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
// 		UploadUrl: "http://localhost/upload",
// 		RequestId: misc.FastUUID().URN(),
// 	}, nil)
// 	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
// 		TrackingId: "randomTrackingId",
// 		RequestId:  "randomRequestId",
// 	}, nil)

// 	asyncDestination := common.AsyncDestinationStruct{
// 		ImportingJobIDs: []int64{1, 2, 3},
// 		FailedJobIDs:    []int64{},
// 		FileName:        "randomFileName.txt",
// 	}
// 	expected := common.AsyncUploadOutput{FailedReason: `{"error":"Jobs flowed over the prescribed limit"}`,
// 		ImportingJobIDs:     []int64{1, 2},
// 		FailedJobIDs:        []int64{3},
// 		ImportingParameters: stdjson.RawMessage{},
// 		ImportingCount:      3,
// 		FailedCount:         1,
// 	}

// 	//making upload function call
// 	recieved := bulkUploader.Upload(&destination, &asyncDestination)
// 	recieved.ImportingParameters = stdjson.RawMessage{}

// 	assert.Equal(t, recieved, expected)
// }

// func TestBingAdsUploadFailedGetBulkUploadUrl(t *testing.T) {
// 	ctrl := gomock.NewController(t)
// 	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
// 	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

// 	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
// 	bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))

// 	asyncDestination := common.AsyncDestinationStruct{
// 		ImportingJobIDs: []int64{1, 2, 3},
// 		FailedJobIDs:    []int64{},
// 		FileName:        "randomFileName.txt",
// 	}
// 	expected := common.AsyncUploadOutput{
// 		FailedJobIDs:   []int64{1, 2, 3},
// 		FailedReason:   "{\"error\" : \"unable to get bulk upload url\"}",
// 		ImportingCount: 0,
// 		FailedCount:    3,
// 		AbortCount:     0,
// 	}
// 	recieved := bulkUploader.Upload(&destination, &asyncDestination)
// 	assert.Equal(t, recieved, expected)
// }

// func TestBingAdsUploadEmptyGetBulkUploadUrl(t *testing.T) {

// 	ctrl := gomock.NewController(t)
// 	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
// 	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

// 	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
// 	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
// 		UploadUrl: "",
// 		RequestId: "",
// 	}, nil)

// 	asyncDestination := common.AsyncDestinationStruct{
// 		ImportingJobIDs: []int64{1, 2, 3},
// 		FailedJobIDs:    []int64{},
// 		FileName:        "randomFileName.txt",
// 	}
// 	expected := common.AsyncUploadOutput{
// 		FailedJobIDs:  []int64{1, 2, 3},
// 		FailedReason:  "{\"error\" : \"getting empty string in upload url or request id\"}",
// 		FailedCount:   3,
// 		DestinationID: destination.ID,
// 	}
// 	recieved := bulkUploader.Upload(&destination, &asyncDestination)
// 	assert.Equal(t, recieved, expected)
// }

// func TestBingAdsUploadFailedUploadBulkFile(t *testing.T) {
// 	ctrl := gomock.NewController(t)
// 	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
// 	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

// 	bingAdsUtilImpl.EXPECT().CreateZipFile(gomock.Any(), gomock.Any()).Return("randomZipFile.path", []int64{1, 2}, []int64{3}, nil)

// 	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
// 	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
// 		UploadUrl: "http://localhost/upload",
// 		RequestId: misc.FastUUID().URN(),
// 	}, nil)
// 	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))

// 	asyncDestination := common.AsyncDestinationStruct{
// 		ImportingJobIDs: []int64{1, 2, 3},
// 		FailedJobIDs:    []int64{},
// 		FileName:        "randomFileName.txt",
// 	}
// 	expected := common.AsyncUploadOutput{
// 		FailedJobIDs:  []int64{1, 2, 3},
// 		FailedReason:  "{\"error\" : \"unable to upload bulk file\"}",
// 		FailedCount:   3,
// 		DestinationID: destination.ID,
// 	}
// 	recieved := bulkUploader.Upload(&destination, &asyncDestination)
// 	assert.Equal(t, recieved, expected)
// }

func TestBingAdsPollSuccessCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second.Abs())

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

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second)

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

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second)

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

	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second)

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

// func TestBingAdsUploadFailedWhileTransformingFile(t *testing.T) {
// 	ctrl := gomock.NewController(t)
// 	bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
// 	bingAdsUtilImpl := mock_bulkservice.NewMockBingAdsUtils(ctrl)

// 	bingAdsUtilImpl.EXPECT().CreateZipFile(gomock.Any(), gomock.Any()).Return("", nil, nil, fmt.Errorf("Error in creating zip file"))

// 	bulkUploader := NewBingAdsBulkUploader(bingAdsService, 10*time.Second, bingAdsUtilImpl)
// 	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
// 		UploadUrl: "http://localhost/upload",
// 		RequestId: misc.FastUUID().URN(),
// 	}, nil)

// 	asyncDestination := common.AsyncDestinationStruct{
// 		ImportingJobIDs: []int64{1, 2, 3},
// 		FailedJobIDs:    []int64{},
// 		FileName:        "randomFileName.txt",
// 	}
// 	expected := common.AsyncUploadOutput{
// 		FailedJobIDs:  []int64{1, 2, 3},
// 		FailedReason:  `got error while transforming the file. Error in creating zip file`,
// 		FailedCount:   3,
// 		DestinationID: destination.ID,
// 	}
// 	recieved := bulkUploader.Upload(&destination, &asyncDestination)
// 	assert.Equal(t, recieved, expected)
// }
