package asyncdestinationmanager

import (
	stdjson "encoding/json"
	"fmt"
	"testing"
	time "time"

	"github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/bingads"
	bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/bingads_sdk"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
)

var destination = backendconfig.DestinationT{
	Name: "BingAdsAudience",
}

// Success scenario
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
	// oauthClient := mock_oauth.NewMockAuthorizer(ctrl)

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
