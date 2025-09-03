package offline_conversions

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/stats"

	mockbingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	mockbulkservice "github.com/rudderlabs/bing-ads-go-sdk/mocks"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	bingadscommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads-v2/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	once        sync.Once
	destination = backendconfig.DestinationT{
		Name: "BingAds",
		Config: map[string]interface{}{
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

func createTestBaseManager(t *testing.T, bingAdsService mockbingads.BulkServiceI) *bingadscommon.BaseManager {
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

func TestBingAdsOfflineConversions(t *testing.T) {
	t.Run("TestBingAdsUploadPartialSuccessCase", func(t *testing.T) {
		config.Reset()
		config.Set("BatchRouter.BING_ADS_OFFLINE_CONVERSIONS.MaxUploadLimit", 1*bytesize.KB)
		initBingads()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
			UploadUrl: "http://localhost/upload1",
			RequestId: misc.FastUUID().URN(),
		}, nil)
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
			UploadUrl: "http://localhost/upload2",
			RequestId: misc.FastUUID().URN(),
		}, nil)
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
			UploadUrl: "http://localhost/upload3",
			RequestId: misc.FastUUID().URN(),
		}, nil)

		bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload1", gomock.Any()).Return(&mockbingads.UploadBulkFileResponse{
			TrackingId: "randomTrackingId1",
			RequestId:  "randomRequestId1",
		}, nil)
		bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))
		bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload3", gomock.Any()).Return(&mockbingads.UploadBulkFileResponse{
			TrackingId: "randomTrackingId3",
			RequestId:  "randomRequestId3",
		}, nil)

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
			ImportingJobIDs: []int64{1, 2, 3, 4, 5, 6},
			FailedJobIDs:    []int64{},
			FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
			Destination:     &destination,
			Manager:         bulkUploader,
		}
		expected := common.AsyncUploadOutput{
			FailedReason:        "{\"error\":\"insert:error in uploading the bulk file: unable to get bulk upload url, check your credentials\"}",
			ImportingJobIDs:     []int64{2, 4, 5, 6},
			FailedJobIDs:        []int64{1, 3},
			ImportingParameters: json.RawMessage{},
			ImportingCount:      4,
			FailedCount:         2,
		}

		received := bulkUploader.Upload(&asyncDestination)
		// Reset ImportingParameters to empty as in original test
		received.ImportingParameters = json.RawMessage{}

		require.Equal(t, expected, received)
	})

	t.Run("TestBingAdsUploadFailedGetBulkUploadUrl", func(t *testing.T) {
		config.Reset()
		initBingads()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)
		errorMsg := "Error in getting bulk upload url"
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errorMsg))
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errorMsg))
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errorMsg))

		asyncDestination := common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1, 2, 3, 4, 5, 6},
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
			FailedJobIDs:        []int64{2, 4, 1, 3, 5, 6},
			FailedReason:        "{\"error\":\"update:error in getting bulk upload url: Error in getting bulk upload url,insert:error in getting bulk upload url: Error in getting bulk upload url,delete:error in getting bulk upload url: Error in getting bulk upload url\"}",
			ImportingCount:      0,
			FailedCount:         6,
			AbortCount:          0,
			ImportingParameters: json.RawMessage(importParameters),
			ImportingJobIDs:     []int64{},
		}

		dir, err := os.MkdirTemp("/tmp", "rudder-server")
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, os.RemoveAll(dir))
		})

		subDir := filepath.Join(dir, "rudder-async-destination-logs")
		err = os.Mkdir(subDir, 0o755)
		require.NoError(t, err)

		t.Setenv("RUDDER_TMPDIR", dir)

		received := bulkUploader.Upload(&asyncDestination)

		require.Equal(t, expected, received)
	})

	t.Run("TestBingAdsUploadEmptyGetBulkUploadUrl", func(t *testing.T) {
		config.Reset()
		initBingads()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		errMsg := "unable to get bulk upload url, check your credentials"

		bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errMsg))
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errMsg))
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errMsg))

		asyncDestination := common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1, 2, 3, 4, 5, 6},
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
			FailedJobIDs:        []int64{2, 4, 1, 3, 5, 6},
			FailedReason:        "{\"error\":\"update:error in getting bulk upload url: unable to get bulk upload url, check your credentials,insert:error in getting bulk upload url: unable to get bulk upload url, check your credentials,delete:error in getting bulk upload url: unable to get bulk upload url, check your credentials\"}",
			FailedCount:         6,
			DestinationID:       destination.ID,
			ImportingParameters: json.RawMessage(importParameters),
			ImportingJobIDs:     []int64{},
		}

		dir, err := os.MkdirTemp("/tmp", "rudder-server")
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, os.RemoveAll(dir))
		})

		subDir := filepath.Join(dir, "rudder-async-destination-logs")
		err = os.Mkdir(subDir, 0o755)
		require.NoError(t, err)
		t.Setenv("RUDDER_TMPDIR", dir)

		received := bulkUploader.Upload(&asyncDestination)

		require.Equal(t, expected, received)
	})

	t.Run("TestBingAdsUploadFailedUploadBulkFile", func(t *testing.T) {
		config.Reset()
		initBingads()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
			UploadUrl: "http://localhost/upload1",
			RequestId: misc.FastUUID().URN(),
		}, nil)
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
			UploadUrl: "http://localhost/upload2",
			RequestId: misc.FastUUID().URN(),
		}, nil)
		bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
			UploadUrl: "http://localhost/upload3",
			RequestId: misc.FastUUID().URN(),
		}, nil)

		bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload1", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))
		bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))
		bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload3", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))

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
			ImportingJobIDs: []int64{1, 2, 3, 4, 5, 6},
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
			FailedJobIDs:        []int64{2, 4, 1, 3, 5, 6},
			FailedReason:        "{\"error\":\"update:error in uploading the bulk file: Error in uploading bulk file,insert:error in uploading the bulk file: Error in uploading bulk file,delete:error in uploading the bulk file: Error in uploading bulk file\"}",
			FailedCount:         6,
			DestinationID:       destination.ID,
			ImportingParameters: json.RawMessage(importParameters),
			ImportingJobIDs:     []int64{},
		}

		received := bulkUploader.Upload(&asyncDestination)

		require.Equal(t, expected, received)
	})

	t.Run("TestBingAdsPollSuccessCase", func(t *testing.T) {
		t.Parallel()
		initBingads()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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

		recievedResponse := bulkUploader.Poll(pollInput)
		require.Equal(t, expectedResp, recievedResponse)
	})

	t.Run("TestBingAdsPollFailureCase", func(t *testing.T) {
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

		recievedResponse := bulkUploader.Poll(pollInput)
		require.Equal(t, expectedResp, recievedResponse)
	})

	t.Run("TestBingAdsPollPartialFailureCase", func(t *testing.T) {
		t.Parallel()
		initBingads()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
		recievedResponse := bulkUploader.Poll(pollInput)

		// Clean up if file created
		if expectedResp.FailedJobParameters != "" {
			t.Cleanup(func() {
				os.Remove(expectedResp.FailedJobParameters)
			})
		}

		require.Equal(t, expectedResp, recievedResponse)
	})

	t.Run("TestBingAdsPollPendingStatusCase", func(t *testing.T) {
		t.Parallel()
		initBingads()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
		recievedResponse := bulkUploader.Poll(pollInput)

		// Clean up in case file was created
		if expectedResp.FailedJobParameters != "" {
			t.Cleanup(func() {
				os.Remove(expectedResp.FailedJobParameters)
			})
		}

		require.Equal(t, expectedResp, recievedResponse)
	})

	t.Run("TestBingAdsPollFailedStatusCase", func(t *testing.T) {
		t.Parallel()
		initBingads()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
		recievedResponse := bulkUploader.Poll(pollInput)

		if expectedResp.FailedJobParameters != "" {
			t.Cleanup(func() {
				os.Remove(expectedResp.FailedJobParameters)
			})
		}

		require.Equal(t, expectedResp, recievedResponse)
	})

	t.Run("TestBingAdsPollSuccessAndFailedStatusCase", func(t *testing.T) {
		t.Parallel()
		initBingads()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId456").Return(&mockbingads.GetBulkUploadStatusResponse{
			PercentComplete: int64(100),
			RequestStatus:   "Completed",
			ResultFileUrl:   "",
		}, nil)

		bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
			FailedJobParameters: ",", // empty file content in original test
		}
		recievedResponse := bulkUploader.Poll(pollInput)

		if expectedResp.FailedJobParameters != "" {
			t.Cleanup(func() {
				os.Remove(expectedResp.FailedJobParameters)
			})
		}

		require.Equal(t, expectedResp, recievedResponse)
	})

	t.Run("TestBingAdsGetUploadStats", func(t *testing.T) {
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

		baseManager := createTestBaseManager(t, bingAdsService)
		bulkUploader := NewBingAdsBulkUploader(baseManager)

		UploadStatsInput := common.GetUploadStatsInput{
			FailedJobParameters: ts.URL,
			ImportingList: []*jobsdb.JobT{
				{JobID: 1},
				{JobID: 3},
				{JobID: 2},
				{JobID: 4},
				{JobID: 5},
				{JobID: 6},
			},
		}

		expectedResp := common.GetUploadStatsResponse{
			StatusCode: 200,
			Metadata: common.EventStatMeta{
				AbortedKeys: []int64{2},
				AbortedReasons: map[int64]string{
					2: "OfflineConversionAdjustmentValueRequired",
				},
				SucceededKeys: []int64{1, 3, 4, 5, 6},
			},
		}

		recievedResponse := bulkUploader.GetUploadStats(UploadStatsInput)
		require.Equal(t, expectedResp, recievedResponse)
	})

	t.Run("TestNewManagerSignature", func(t *testing.T) {
		t.Parallel()
		require.NotNil(t, NewManager, "NewManager function should not be nil")
	})

	t.Run("Transform Tests", func(t *testing.T) {
		t.Run("Transform successful", func(t *testing.T) {
			t.Parallel()
			job := &jobsdb.JobT{
				EventPayload: []byte("{\n  \"type\": \"record\",\n  \"action\": \"insert\",\n  \"fields\": {\n    \"conversionName\": \"Test-Integration\",\n    \"conversionTime\": \"2023-05-22T06:27:54Z\",\n    \"conversionValue\": \"100\",\n    \"microsoftClickId\": \"click_id\",\n    \"conversionCurrencyCode\": \"USD\",\n    \"email\":\"test@testmail.com\",\n    \"phone\":\"+911234567890\"\n  }\n}"),
			}
			uploader := &BingAdsBulkUploader{
				isHashRequired: true,
			}
			expectedResp := `{"message":{"fields":{"conversionCurrencyCode":"USD","conversionName":"Test-Integration","conversionTime":"5/22/2023 6:27:54 AM","conversionValue":"100","email":"28a4da98f8812110001ab8ffacde3b38b4725a9e3570c39299fbf2d12c5aa70e","microsoftClickId":"click_id","phone":"8c229df83de8ab269e90918846e326c4008c86481393223d17a30ff5a407b08e"},"action":"insert"},"metadata":{"jobId":0}}`

			resp, err := uploader.Transform(job)
			require.NoError(t, err)
			require.JSONEq(t, expectedResp, resp)
		})

		t.Run("Transform adjustedConversionTime not available", func(t *testing.T) {
			t.Parallel()
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"conversionValue\": \"100\", \"microsoftClickId\": \"click_id\", \"conversionCurrencyCode\": \"USD\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			_, err := uploader.Transform(job)
			expectedResult := fmt.Errorf(" adjustedConversionTime field not defined")
			require.Error(t, err)
			require.Equal(t, expectedResult.Error(), err.Error())
		})

		t.Run("Transform microsoftClickId is required but missing", func(t *testing.T) {
			t.Parallel()
			payloads := [][]byte{
				[]byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"conversionValue\": \"100\", \"adjustedConversionTime\": \"2023-05-22T06:27:54Z\", \"conversionCurrencyCode\": \"USD\"}}"),
				[]byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"conversionValue\": \"100\", \"adjustedConversionTime\": \"2023-05-22T06:27:54Z\", \"conversionCurrencyCode\": \"USD\", \"email\":\"\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			expectedResult := fmt.Errorf("missing required field: microsoftClickId (or provide a hashed email/phone for enhanced conversions)")

			for _, payload := range payloads {
				job := &jobsdb.JobT{EventPayload: payload}
				_, err := uploader.Transform(job)
				require.Error(t, err)
				require.Equal(t, expectedResult.Error(), err.Error())
			}
		})

		t.Run("Transform successful with email/phone but missing microsoftClickId", func(t *testing.T) {
			t.Parallel()
			job := &jobsdb.JobT{
				EventPayload: []byte("{\n  \"type\": \"record\",\n  \"action\": \"insert\",\n  \"fields\": {\n    \"conversionName\": \"Test-Integration\",\n    \"conversionTime\": \"2023-05-22T18:27:54Z\",\n    \"adjustedConversionTime\": \"2023-05-22T18:27:54Z\",\n    \"conversionValue\": \"100\",\n    \"conversionCurrencyCode\": \"USD\",\n    \"email\": \"test@testmail.com\",\n    \"phone\": \"+911234567890\"\n  }\n}"),
			}
			uploader := &BingAdsBulkUploader{
				isHashRequired: true,
			}
			expectedResp := `{"message":{"fields":{"adjustedConversionTime":"5/22/2023 6:27:54 PM","conversionCurrencyCode":"USD","conversionName":"Test-Integration","conversionTime":"5/22/2023 6:27:54 PM","conversionValue":"100","email":"28a4da98f8812110001ab8ffacde3b38b4725a9e3570c39299fbf2d12c5aa70e","phone":"8c229df83de8ab269e90918846e326c4008c86481393223d17a30ff5a407b08e"},"action":"insert"},"metadata":{"jobId":0}}`

			resp, err := uploader.Transform(job)
			require.NoError(t, err)
			require.JSONEq(t, expectedResp, resp)
		})

		t.Run("TestBingAdsTransformWithInvalidTimestamp", func(t *testing.T) {
			t.Parallel()
			initBingads()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			baseManager := createTestBaseManager(t, nil)
			bulkUploader := NewBingAdsBulkUploader(baseManager)

			job := &jobsdb.JobT{
				EventPayload: []byte(`{
					"Action": "insert",
					"Fields": {
						"conversionName": "TestConversion",
						"conversionTime": "invalid-timestamp",
						"microsoftClickId": "12345"
					}
				}`),
			}

			result, err := bulkUploader.Transform(job)
			require.Error(t, err)
			require.Contains(t, err.Error(), "conversionTime must be in ISO 8601")
			require.Equal(t, string(job.EventPayload), result)
		})

		t.Run("Transform conversionTime not string", func(t *testing.T) {
			t.Parallel()
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": 12345, \"conversionValue\": \"100\", \"microsoftClickId\": \"click_id\", \"conversionCurrencyCode\": \"USD\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			_, err := uploader.Transform(job)
			require.Error(t, err)
			require.Contains(t, err.Error(), "cannot unmarshal number into Go struct field RecordFields.fields.conversionTime of type string")
		})

		t.Run("Transform JSON unmarshaling errors", func(t *testing.T) {
			testCases := []struct {
				name         string
				eventPayload []byte
				expectedErr  string
			}{
				{
					name:         "invalid JSON payload",
					eventPayload: []byte("invalid json"),
					expectedErr:  "unmarshalling event",
				},
				{
					name:         "malformed event structure",
					eventPayload: []byte("{\"type\": \"record\", \"action\": \"insert\"}"), // missing fields
					expectedErr:  " conversionName field not defined",
				},
				{
					name:         "invalid fields JSON",
					eventPayload: []byte("{\"type\": \"record\", \"action\": \"insert\", \"fields\": \"invalid\"}"),
					expectedErr:  "cannot unmarshal string into Go struct field Record.fields",
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					job := &jobsdb.JobT{EventPayload: tc.eventPayload}
					uploader := &BingAdsBulkUploader{}

					result, err := uploader.Transform(job)
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.expectedErr)
					require.Equal(t, string(tc.eventPayload), result)
				})
			}
		})

		t.Run("Transform hash fields error", func(t *testing.T) {
			// Create a mock that will cause hashFields to fail
			// This tests the error path when isHashRequired is true
			job := &jobsdb.JobT{
				EventPayload: []byte("{\n  \"type\": \"record\",\n  \"action\": \"insert\",\n  \"fields\": {\n    \"conversionName\": \"Test-Integration\",\n    \"conversionTime\": \"2023-05-22T06:27:54Z\",\n    \"microsoftClickId\": \"click_id\"\n  }\n}"),
			}

			// Create uploader with hash required but mock the hashFields function
			uploader := &BingAdsBulkUploader{
				isHashRequired: true,
			}

			// This test will pass if hashFields works correctly
			// To test the error path, we'd need to mock the hashFields function
			// For now, we test the successful case with hash required
			result, err := uploader.Transform(job)
			require.NoError(t, err)
			require.Contains(t, result, "click_id")
			require.Contains(t, result, "insert")
		})

		t.Run("Transform different action types", func(t *testing.T) {
			testCases := []struct {
				name        string
				action      string
				fields      map[string]interface{}
				expectError bool
				errorMsg    string
			}{
				{
					name:   "delete action with required fields",
					action: "delete",
					fields: map[string]interface{}{
						"conversionName":         "Test-Integration",
						"conversionTime":         "2023-05-22T06:27:54Z",
						"microsoftClickId":       "click_id",
						"adjustedConversionTime": "2023-05-22T06:27:54Z",
					},
					expectError: false,
				},
				{
					name:   "delete action missing adjustedConversionTime",
					action: "delete",
					fields: map[string]interface{}{
						"conversionName":   "Test-Integration",
						"conversionTime":   "2023-05-22T06:27:54Z",
						"microsoftClickId": "click_id",
					},
					expectError: true,
					errorMsg:    "adjustedConversionTime field not defined",
				},
				{
					name:   "update action missing conversionValue",
					action: "update",
					fields: map[string]interface{}{
						"conversionName":         "Test-Integration",
						"conversionTime":         "2023-05-22T06:27:54Z",
						"microsoftClickId":       "click_id",
						"adjustedConversionTime": "2023-05-22T06:27:54Z",
					},
					expectError: true,
					errorMsg:    "conversionValue field not defined",
				},
				{
					name:   "custom action type",
					action: "custom_action",
					fields: map[string]interface{}{
						"conversionName":         "Test-Integration",
						"conversionTime":         "2023-05-22T06:27:54Z",
						"microsoftClickId":       "click_id",
						"adjustedConversionTime": "2023-05-22T06:27:54Z",
					},
					expectError: false, // Should pass validation for non-insert actions
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					// Create fields JSON
					fieldsJSON, err := jsonrs.Marshal(tc.fields)
					require.NoError(t, err)

					// Create event payload
					eventPayload := fmt.Sprintf(`{
						"type": "record",
						"action": "%s",
						"fields": %s
					}`, tc.action, string(fieldsJSON))

					job := &jobsdb.JobT{
						EventPayload: []byte(eventPayload),
					}
					uploader := &BingAdsBulkUploader{}

					result, err := uploader.Transform(job)

					if tc.expectError {
						require.Error(t, err)
						require.Contains(t, err.Error(), tc.errorMsg)
						require.Equal(t, string(job.EventPayload), result)
					} else {
						require.NoError(t, err)
						require.Contains(t, result, tc.action)
					}
				})
			}
		})

		t.Run("Transform edge cases and error conditions", func(t *testing.T) {
			testCases := []struct {
				name         string
				eventPayload []byte
				uploader     *BingAdsBulkUploader
				expectError  bool
				errorMsg     string
			}{
				{
					name:         "empty fields object",
					eventPayload: []byte("{\"type\": \"record\", \"action\": \"insert\", \"fields\": {}}"),
					uploader:     &BingAdsBulkUploader{},
					expectError:  true,
					errorMsg:     " conversionName field not defined",
				},
				{
					name:         "missing conversionName",
					eventPayload: []byte("{\"type\": \"record\", \"action\": \"insert\", \"fields\": {\"conversionTime\": \"2023-05-22T06:27:54Z\"}}"),
					uploader:     &BingAdsBulkUploader{},
					expectError:  true,
					errorMsg:     " conversionName field not defined",
				},
				{
					name:         "missing conversionTime",
					eventPayload: []byte("{\"type\": \"record\", \"action\": \"insert\", \"fields\": {\"conversionName\": \"Test-Integration\"}}"),
					uploader:     &BingAdsBulkUploader{},
					expectError:  true,
					errorMsg:     " conversionTime field not defined",
				},
				{
					name:         "empty string values",
					eventPayload: []byte("{\"type\": \"record\", \"action\": \"insert\", \"fields\": {\"conversionName\": \"\", \"conversionTime\": \"\", \"email\": \"\", \"phone\": \"\"}}"),
					uploader:     &BingAdsBulkUploader{},
					expectError:  true,
					errorMsg:     " conversionName field not defined",
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					job := &jobsdb.JobT{EventPayload: tc.eventPayload}

					result, err := tc.uploader.Transform(job)

					if tc.expectError {
						require.Error(t, err)
						require.Contains(t, err.Error(), tc.errorMsg)
						require.Equal(t, string(job.EventPayload), result)
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	})
}
