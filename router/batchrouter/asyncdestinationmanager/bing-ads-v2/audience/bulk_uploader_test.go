package audience

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	bingads_sdk "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	mockbulkservice "github.com/rudderlabs/bing-ads-go-sdk/mocks"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-go-kit/stats"
	bingadscommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads-v2/common"
)

var (
	once        sync.Once
	destination = backendconfig.DestinationT{
		Name: "BingAds",
		Config: map[string]interface{}{
			"audienceId":        "audience_id",
			"customerAccountId": "customer_account_id",
			"customerId":        "customer_id",
			"rudderAccountId":   "rudder_account_id",
		},
		WorkspaceID: "workspace_id",
	}
)

var currentDir, _ = os.Getwd()

func initBingads() {
	once.Do(func() {
		logger.Reset()
		misc.Init()
	})
}

func createTestBaseManager(t *testing.T, bingAdsService bingads_sdk.BulkServiceI) *bingadscommon.BaseManager {
	t.Helper()
	return &bingadscommon.BaseManager{
		Logger:          logger.NOP,
		StatsFactory:    stats.NOP,
		DestinationName: "BING_ADS",
		Service:         bingAdsService,
		DestConfig: &bingadscommon.BaseDestinationConfig{
			IsHashRequired: true,
		},
	}
}

func TestBingAdsUploadPartialSuccessCase(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload1",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload2",
		RequestId: misc.FastUUID().URN(),
	}, nil)

	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload1", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
		TrackingId: "randomTrackingId1",
		RequestId:  "randomRequestId1",
	}, nil)
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0o755)
	require.NoError(t, err)

	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
		Destination:     &destination,
		Manager:         bulkUploader,
	}

	expected := common.AsyncUploadOutput{
		FailedReason:        "{\"error\":\"Add:error in uploading the bulk file: unable to get bulk upload url, check your credentials\"}",
		ImportingJobIDs:     []int64{3, 4},
		FailedJobIDs:        []int64{1, 2},
		ImportingParameters: json.RawMessage{},
		ImportingCount:      2,
		FailedCount:         2,
	}

	received := bulkUploader.Upload(&asyncDestination)
	received.ImportingParameters = json.RawMessage{}

	require.NotEmpty(t, received.FailedReason, "Expected failure reason but got none")
	require.Equal(t, expected, received)
}

func TestBingAdsUploadFailedGetBulkUploadUrl(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0o755)
	require.NoError(t, err)

	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
		Destination:     &destination,
		Manager:         bulkUploader,
	}

	var parameters common.ImportParameters
	parameters.ImportId = ""
	importParameters, err := jsonrs.Marshal(parameters)
	require.NoError(t, err)

	expected := common.AsyncUploadOutput{
		FailedJobIDs:        []int64{3, 4, 1, 2},
		FailedReason:        "{\"error\":\"Remove:error in getting bulk upload url: Error in getting bulk upload url,Add:error in getting bulk upload url: Error in getting bulk upload url\"}",
		ImportingCount:      0,
		FailedCount:         4,
		AbortCount:          0,
		ImportingParameters: json.RawMessage(importParameters),
		ImportingJobIDs:     nil,
	}

	received := bulkUploader.Upload(&asyncDestination)

	require.Equal(t, expected, received)
}

func TestBingAdsUploadEmptyGetBulkUploadUrl(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0o755)
	require.NoError(t, err)

	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
		Destination:     &destination,
		Manager:         bulkUploader,
	}

	var parameters common.ImportParameters
	parameters.ImportId = ""
	importParameters, err := jsonrs.Marshal(parameters)
	require.NoError(t, err)

	expected := common.AsyncUploadOutput{
		FailedJobIDs:        []int64{3, 4, 1, 2},
		FailedReason:        "{\"error\":\"Remove:error in getting bulk upload url: unable to get bulk upload url, check your credentials,Add:error in getting bulk upload url: unable to get bulk upload url, check your credentials\"}",
		FailedCount:         4,
		DestinationID:       destination.ID,
		ImportingParameters: json.RawMessage(importParameters),
		ImportingJobIDs:     nil,
	}

	received := bulkUploader.Upload(&asyncDestination)

	require.Equal(t, expected, received)
}

func TestBingAdsUploadFailedUploadBulkFile(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload1",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload2",
		RequestId: misc.FastUUID().URN(),
	}, nil)

	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload1", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0o755)
	require.NoError(t, err)

	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
		Destination:     &destination,
		Manager:         bulkUploader,
	}

	var parameters common.ImportParameters
	parameters.ImportId = ""
	importParameters, err := jsonrs.Marshal(parameters)
	require.NoError(t, err)

	expected := common.AsyncUploadOutput{
		FailedJobIDs:        []int64{3, 4, 1, 2},
		FailedReason:        "{\"error\":\"Remove:error in uploading the bulk file: Error in uploading bulk file,Add:error in uploading the bulk file: Error in uploading bulk file\"}",
		FailedCount:         4,
		DestinationID:       destination.ID,
		ImportingParameters: json.RawMessage(importParameters),
		ImportingJobIDs:     nil,
	}

	received := bulkUploader.Upload(&asyncDestination)

	require.Equal(t, expected, received)
}

func TestBingAdsPollSuccessCase(t *testing.T) {
	t.Parallel()
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(100),
		RequestStatus:   "Completed",
		ResultFileUrl:   "http://dummyurl.com",
	}, nil)

	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}
	expectedResp := common.PollStatusResponse{
		Complete:   true,
		StatusCode: 200,
	}

	receivedResponse := bulkUploader.Poll(pollInput)

	require.Equal(t, expectedResp, receivedResponse)
}

func TestBingAdsPollFailureCase(t *testing.T) {
	t.Parallel()
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(nil, fmt.Errorf("failed to get bulk upload status:"))

	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}
	expectedResp := common.PollStatusResponse{
		StatusCode: 500,
		HasFailed:  true,
	}

	receivedResponse := bulkUploader.Poll(pollInput)

	require.Equal(t, expectedResp, receivedResponse)
}

func TestBingAdsPollPartialFailureCase(t *testing.T) {
	t.Parallel()
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(100),
		RequestStatus:   "CompletedWithErrors",
		ResultFileUrl:   "https://dummy.url.com",
	}, nil)

	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}

	expectedResp := common.PollStatusResponse{
		Complete:            true,
		StatusCode:          200,
		HasFailed:           true,
		FailedJobParameters: "https://dummy.url.com",
	}

	receivedResponse := bulkUploader.Poll(pollInput)

	require.Equal(t, expectedResp, receivedResponse)

	// Remove file if created (safe even if it does not exist)
	if expectedResp.FailedJobParameters != "" {
		t.Cleanup(func() {
			os.Remove(expectedResp.FailedJobParameters)
		})
	}
}

func TestBingAdsPollPendingStatusCase(t *testing.T) {
	t.Parallel()
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(0),
		RequestStatus:   "InProgress",
		ResultFileUrl:   "",
	}, nil)

	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}

	expectedResp := common.PollStatusResponse{
		InProgress: true,
		StatusCode: 200,
	}

	receivedResponse := bulkUploader.Poll(pollInput)

	require.Equal(t, expectedResp, receivedResponse)

	if expectedResp.FailedJobParameters != "" {
		t.Cleanup(func() {
			os.Remove(expectedResp.FailedJobParameters)
		})
	}
}

func TestBingAdsPollFailedStatusCase(t *testing.T) {
	t.Parallel()
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(0),
		RequestStatus:   "Failed",
		ResultFileUrl:   "",
	}, nil)

	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId123",
	}

	expectedResp := common.PollStatusResponse{
		HasFailed:  true,
		StatusCode: 500,
	}

	receivedResponse := bulkUploader.Poll(pollInput)

	require.Equal(t, expectedResp, receivedResponse)

	if expectedResp.FailedJobParameters != "" {
		t.Cleanup(func() {
			os.Remove(expectedResp.FailedJobParameters)
		})
	}
}

func TestBingAdsPollSuccessAndFailedStatusCase(t *testing.T) {
	t.Parallel()
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId456").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(100),
		RequestStatus:   "Completed",
		ResultFileUrl:   "",
	}, nil)
	bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
		PercentComplete: int64(0),
		RequestStatus:   "Failed",
		ResultFileUrl:   "",
	}, nil)

	pollInput := common.AsyncPoll{
		ImportId: "dummyRequestId456,dummyRequestId123",
	}

	expectedResp := common.PollStatusResponse{
		HasFailed:           true,
		StatusCode:          500,
		FailedJobParameters: ",", // empty file as string
	}

	receivedResponse := bulkUploader.Poll(pollInput)

	require.Equal(t, expectedResp, receivedResponse)

	if expectedResp.FailedJobParameters != "" {
		t.Cleanup(func() {
			os.Remove(expectedResp.FailedJobParameters)
		})
	}
}

func TestBingAdsGetUploadStats(t *testing.T) {
	t.Parallel()
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	errorsTemplateFilePath := filepath.Join(currentDir, "testdata/status-check.zip")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename='uploadstatus.zip'")
		http.ServeFile(w, r, errorsTemplateFilePath)
	}))
	defer ts.Close()

	modifiedURL := ts.URL
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	UploadStatsInput := common.GetUploadStatsInput{
		FailedJobParameters: modifiedURL,
		ImportingList: []*jobsdb.JobT{
			{JobID: 5},
			{JobID: 6},
			{JobID: 7},
		},
	}

	expectedResp := common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata: common.EventStatMeta{
			AbortedKeys: []int64{6},
			AbortedReasons: map[int64]string{
				6: "EmailMustBeHashed",
			},
			SucceededKeys: []int64{5, 7},
		},
	}

	receivedResponse := bulkUploader.GetUploadStats(UploadStatsInput)

	require.Equal(t, expectedResp.StatusCode, receivedResponse.StatusCode)
	require.Equal(t, len(expectedResp.Metadata.AbortedKeys), len(receivedResponse.Metadata.AbortedKeys))
}

func TestBingAdsGetUploadStatsWrongAudienceId(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	csvPath := "testdata/BulkUpload-02-28-2024-c7a38716-4d65-44a7-bf28-8879ab9b1da0-Results.csv"
	zipPath := "testdata/BulkUpload-02-28-2024-c7a38716-4d65-44a7-bf28-8879ab9b1da0-Results.zip"

	err := zipCSVFile(csvPath, zipPath)
	require.NoError(t, err)

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	errorsTemplateFilePath := filepath.Join(currentDir, zipPath)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename='status-wrong-audience.zip'")
		http.ServeFile(w, r, errorsTemplateFilePath)
	}))
	defer ts.Close()
	modifiedURL := ts.URL

	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	UploadStatsInput := common.GetUploadStatsInput{
		FailedJobParameters: modifiedURL,
		ImportingList: []*jobsdb.JobT{
			{JobID: 1},
		},
	}

	expectedResp := common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata: common.EventStatMeta{
			AbortedKeys: []int64{1},
			AbortedReasons: map[int64]string{
				1: "InvalidCustomerListId",
			},
			SucceededKeys: []int64{},
		},
	}

	receivedResponse := bulkUploader.GetUploadStats(UploadStatsInput)

	t.Cleanup(func() {
		os.Remove(zipPath)
	})

	require.Equal(t, expectedResp.StatusCode, receivedResponse.StatusCode)
	require.Equal(t, len(expectedResp.Metadata.AbortedKeys), len(receivedResponse.Metadata.AbortedKeys))
}

func TestNewManagerSignature(t *testing.T) {
	t.Parallel()
	require.NotNil(t, NewManager, "NewManager should not be nil")
}

func TestBingAdsUploadNoTrackingId(t *testing.T) {
	initBingads()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
	baseManager := createTestBaseManager(t, bingAdsService)
	bulkUploader := NewBingAdsBulkUploader(baseManager)

	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)
	bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
		UploadUrl: "http://localhost/upload",
		RequestId: misc.FastUUID().URN(),
	}, nil)

	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(nil, fmt.Errorf("unable to upload bulk file, check your credentials"))
	bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(nil, fmt.Errorf("unable to upload bulk file, check your credentials"))

	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	subDir := filepath.Join(dir, "rudder-async-destination-logs")
	err = os.Mkdir(subDir, 0o755)
	require.NoError(t, err)

	t.Setenv("RUDDER_TMPDIR", dir)

	asyncDestination := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3, 4},
		FailedJobIDs:    []int64{},
		FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
		Destination:     &destination,
		Manager:         bulkUploader,
	}

	expected := common.AsyncUploadOutput{
		FailedReason:        "{\"error\":\"Remove:error in uploading the bulk file: unable to upload bulk file, check your credentials,Add:error in uploading the bulk file: unable to upload bulk file, check your credentials\"}",
		FailedJobIDs:        []int64{3, 4, 1, 2},
		ImportingParameters: json.RawMessage{},
		ImportingCount:      0,
		FailedCount:         4,
		ImportingJobIDs:     nil,
	}

	received := bulkUploader.Upload(&asyncDestination)
	received.ImportingParameters = json.RawMessage{}

	require.Equal(t, expected, received)
}

func zipCSVFile(csvFilePath, zipFilePath string) error {
	zipFile, err := os.Create(zipFilePath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	csvFileName := filepath.Base(csvFilePath)
	zipEntryWriter, err := zipWriter.Create(csvFileName)
	if err != nil {
		return err
	}

	csvFile, err := os.Open(csvFilePath)
	if err != nil {
		return err
	}

	_, err = io.Copy(zipEntryWriter, csvFile)
	if err != nil {
		return err
	}

	return zipWriter.Close()
}
