package offline_conversions

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mockbingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	mockbulkservice "github.com/rudderlabs/bing-ads-go-sdk/mocks"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	MockAuthorizer "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	once        sync.Once
	destination = backendconfig.DestinationT{
		Name: "BingAds",
		Config: map[string]any{
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

type bingAdsFixture struct {
	ctrl           *gomock.Controller
	bingAdsService *mockbulkservice.MockBulkServiceI
}

func newBingAdsFixture(t *testing.T) *bingAdsFixture {
	t.Helper()
	config.Reset()
	config.Set("BatchRouter.BING_ADS_OFFLINE_CONVERSIONS.MaxUploadLimit", 1*bytesize.KB)
	t.Cleanup(config.Reset)
	initBingads()
	ctrl := gomock.NewController(t)
	return &bingAdsFixture{
		ctrl:           ctrl,
		bingAdsService: mockbulkservice.NewMockBulkServiceI(ctrl),
	}
}

// setupRudderTmpDir points RUDDER_TMPDIR to a fresh directory that contains the async destination logs folder.
func setupRudderTmpDir(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "rudder-async-destination-logs"), 0o755), "Creating the directory 'something'")
	t.Setenv("RUDDER_TMPDIR", dir)
}

func TestBingAdsOfflineConversions(t *testing.T) {
	t.Run("Bing ads", func(t *testing.T) {
		t.Run("TestBingAdsUploadPartialSuccessCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload1",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload2",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload3",
				RequestId: misc.FastUUID().URN(),
			}, nil)

			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload1", gomock.Any()).Return(&mockbingads.UploadBulkFileResponse{
				TrackingId: "randomTrackingId1",
				RequestId:  "randomRequestId1",
			}, nil)
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload3", gomock.Any()).Return(&mockbingads.UploadBulkFileResponse{
				TrackingId: "randomTrackingId3",
				RequestId:  "randomRequestId3",
			}, nil)
			setupRudderTmpDir(t)

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

			// making upload function call
			received := bulkUploader.Upload(t.Context(), &asyncDestination)
			received.ImportingParameters = json.RawMessage{}

			require.Equal(t, expected, received)
		})

		t.Run("TestBingAdsUploadFailedGetBulkUploadUrl", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)
			errorMsg := "Error in getting bulk upload url"
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errorMsg))
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errorMsg))
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errorMsg))

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
			require.NoError(t, err, "unmarshalling parameters")
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{2, 4, 1, 3, 5, 6},
				FailedReason:        "{\"error\":\"update:error in getting bulk upload url: Error in getting bulk upload url,insert:error in getting bulk upload url: Error in getting bulk upload url,delete:error in getting bulk upload url: Error in getting bulk upload url\"}",
				ImportingCount:      0,
				FailedCount:         6,
				AbortCount:          0,
				ImportingParameters: json.RawMessage(importParameters),
			}
			setupRudderTmpDir(t)
			received := bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})

		t.Run("TestBingAdsUploadEmptyGetBulkUploadUrl", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)
			errMsg := "unable to get bulk upload url, check your credentials"
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errMsg))

			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errMsg))

			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, errors.New(errMsg))

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
			require.NoError(t, err, "removing temporary directory")
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{2, 4, 1, 3, 5, 6},
				FailedReason:        "{\"error\":\"update:error in getting bulk upload url: unable to get bulk upload url, check your credentials,insert:error in getting bulk upload url: unable to get bulk upload url, check your credentials,delete:error in getting bulk upload url: unable to get bulk upload url, check your credentials\"}",
				FailedCount:         6,
				DestinationID:       destination.ID,
				ImportingParameters: json.RawMessage(importParameters),
			}

			setupRudderTmpDir(t)
			received := bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})

		t.Run("TestBingAdsUploadFailedUploadBulkFile", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload1",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload2",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&mockbingads.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload3",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload1", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload3", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))

			setupRudderTmpDir(t)

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
			require.NoError(t, err, "removing temporary directory")
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{2, 4, 1, 3, 5, 6},
				FailedReason:        "{\"error\":\"update:error in uploading the bulk file: Error in uploading bulk file,insert:error in uploading the bulk file: Error in uploading bulk file,delete:error in uploading the bulk file: Error in uploading bulk file\"}",
				FailedCount:         6,
				DestinationID:       destination.ID,
				ImportingParameters: json.RawMessage(importParameters),
			}
			received := bulkUploader.Upload(t.Context(), &asyncDestination)

			require.Equal(t, expected, received)
		})

		t.Run("TestBingAdsPollSuccessCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
			recievedResponse := bulkUploader.Poll(t.Context(), pollInput)
			require.Equal(t, expectedResp, recievedResponse)
		})

		t.Run("TestBingAdsPollFailureCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(nil, fmt.Errorf("failed to get bulk upload status:"))
			pollInput := common.AsyncPoll{
				ImportId: "dummyRequestId123",
			}
			expectedResp := common.PollStatusResponse{
				StatusCode: 500,
				HasFailed:  true,
			}
			recievedResponse := bulkUploader.Poll(t.Context(), pollInput)
			require.Equal(t, expectedResp, recievedResponse)
		})

		t.Run("TestBingAdsPollPartialFailureCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
			recievedResponse := bulkUploader.Poll(t.Context(), pollInput)

			os.Remove(expectedResp.FailedJobParameters)

			require.Equal(t, expectedResp, recievedResponse)
		})

		t.Run("TestBingAdsPollPendingStatusCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
			recievedResponse := bulkUploader.Poll(t.Context(), pollInput)

			os.Remove(expectedResp.FailedJobParameters)

			require.Equal(t, expectedResp, recievedResponse)
		})

		t.Run("TestBingAdsPollFailedStatusCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
			recievedResponse := bulkUploader.Poll(t.Context(), pollInput)

			os.Remove(expectedResp.FailedJobParameters)

			require.Equal(t, expectedResp, recievedResponse)
		})

		t.Run("TestBingAdsPollSuccessAndFailedStatusCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId456").Return(&mockbingads.GetBulkUploadStatusResponse{
				PercentComplete: int64(100),
				RequestStatus:   "Completed",
				ResultFileUrl:   "",
			}, nil)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&mockbingads.GetBulkUploadStatusResponse{
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
				FailedJobParameters: ",", // empty file
			}
			recievedResponse := bulkUploader.Poll(t.Context(), pollInput)

			os.Remove(expectedResp.FailedJobParameters)

			require.Equal(t, expectedResp, recievedResponse)
		})

		t.Run("TestBingAdsGetUploadStats", func(t *testing.T) {
			f := newBingAdsFixture(t)
			errorsTemplateFilePath := filepath.Join(currentDir, "testdata/status-check.zip") // Path of the source file
			// Create a test server with a custom handler function
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Set the appropriate headers for a zip file response
				w.Header().Set("Content-Type", "application/zip")
				w.Header().Set("Content-Disposition", "attachment; filename='uploadstatus.zip'")
				http.ServeFile(w, r, errorsTemplateFilePath)
			}))
			defer ts.Close()
			modifiedURL := ts.URL // Use the test server URL
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, true)

			UploadStatsInput := common.GetUploadStatsInput{
				FailedJobParameters: modifiedURL,
				ImportingList: []*jobsdb.JobT{
					{
						JobID: 1,
					},
					{
						JobID: 3,
					},
					{
						JobID: 2,
					},
					{
						JobID: 4,
					},
					{
						JobID: 5,
					},
					{
						JobID: 6,
					},
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

		t.Run("TestNewManagerInternal", func(t *testing.T) {
			f := newBingAdsFixture(t)
			oauthV2Service := MockAuthorizer.NewMockOAuthHandler(f.ctrl)
			oauthV2Service.EXPECT().FetchToken(gomock.Any()).Return(
				json.RawMessage(`
							{
							"AccessToken": "dummyacesstoken",
							"RefreshToken": "dummyRefreshToken",
							"Developer_token": "dummyDeveloperToken",
							"ExpirationDate": "2023-01-31T23:59:59.999Z"
							}`),
				nil,
			)

			bingAdsUploader, err := newManagerInternal(logger.NOP, stats.NOP, &destination, oauthV2Service)
			require.NoError(t, err)
			require.NotNil(t, bingAdsUploader)
		})

		t.Run("Transform() Test -> successful ", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\n  \"type\": \"record\",\n  \"action\": \"insert\",\n  \"fields\": {\n    \"conversionName\": \"Test-Integration\",\n    \"conversionTime\": \"2023-05-22T06:27:54Z\",\n    \"conversionValue\": \"100\",\n    \"microsoftClickId\": \"click_id\",\n    \"conversionCurrencyCode\": \"USD\",\n    \"email\":\"test@testmail.com\",\n    \"phone\":\"+911234567890\"\n  }\n}"),
			}
			uploader := &BingAdsBulkUploader{
				isHashRequired: true,
			}
			expectedResp := `{"message":{"fields":{"conversionCurrencyCode":"USD","conversionName":"Test-Integration","conversionTime":"5/22/2023 6:27:54 AM","conversionValue":"100","email":"28a4da98f8812110001ab8ffacde3b38b4725a9e3570c39299fbf2d12c5aa70e","microsoftClickId":"click_id","phone":"8c229df83de8ab269e90918846e326c4008c86481393223d17a30ff5a407b08e"},"action":"insert"},"metadata":{"jobId":0}}`
			// Execute
			resp, err := uploader.Transform(job)
			require.Equal(t, expectedResp, resp)
			require.NoError(t, err)
		})

		t.Run("Transform() Test -> adjustedConversionTime not available", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"conversionValue\": \"100\", \"microsoftClickId\": \"click_id\", \"conversionCurrencyCode\": \"USD\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			// Execute
			_, err := uploader.Transform(job)
			expectedResult := fmt.Errorf(" adjustedConversionTime field not defined")
			require.Equal(t, expectedResult.Error(), err.Error())
		})

		t.Run("Transform() Test -> update reformats and preserves adjustedConversionTime", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"adjustedConversionTime\": \"2023-05-22T18:27:54Z\", \"conversionValue\": \"100\", \"microsoftClickId\": \"click_id\", \"conversionCurrencyCode\": \"USD\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			expectedResp := `{"message":{"fields":{"adjustedConversionTime":"5/22/2023 6:27:54 PM","conversionCurrencyCode":"USD","conversionName":"Test-Integration","conversionTime":"5/22/2023 6:27:54 AM","conversionValue":"100","microsoftClickId":"click_id"},"action":"update"},"metadata":{"jobId":0}}`
			// Execute
			resp, err := uploader.Transform(job)
			require.NoError(t, err)
			require.Equal(t, expectedResp, resp)
		})

		t.Run("Transform() Test -> insert ignores a malformed adjustedConversionTime", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"insert\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"adjustedConversionTime\": \"not-a-timestamp\", \"conversionValue\": \"100\", \"microsoftClickId\": \"click_id\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			// insert does not validate adjustedConversionTime, so the malformed value
			// does not cause an error; it is left untouched (and ignored downstream).
			expectedResp := `{"message":{"fields":{"adjustedConversionTime":"not-a-timestamp","conversionName":"Test-Integration","conversionTime":"5/22/2023 6:27:54 AM","conversionValue":"100","microsoftClickId":"click_id"},"action":"insert"},"metadata":{"jobId":0}}`
			// Execute
			resp, err := uploader.Transform(job)
			require.NoError(t, err)
			require.Equal(t, expectedResp, resp)
		})

		t.Run("Transform() Test -> insert leaves a valid adjustedConversionTime untouched", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"insert\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"adjustedConversionTime\": \"2023-05-22T18:27:54Z\", \"conversionValue\": \"100\", \"microsoftClickId\": \"click_id\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			// conversionTime is reformatted; adjustedConversionTime is NOT reformatted
			// on insert (it is not validated), so it stays in its original form.
			expectedResp := `{"message":{"fields":{"adjustedConversionTime":"2023-05-22T18:27:54Z","conversionName":"Test-Integration","conversionTime":"5/22/2023 6:27:54 AM","conversionValue":"100","microsoftClickId":"click_id"},"action":"insert"},"metadata":{"jobId":0}}`
			// Execute
			resp, err := uploader.Transform(job)
			require.NoError(t, err)
			require.Equal(t, expectedResp, resp)
		})

		t.Run("Transform() Test -> update errors naming a malformed adjustedConversionTime", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"adjustedConversionTime\": \"not-a-timestamp\", \"conversionValue\": \"100\", \"microsoftClickId\": \"click_id\", \"conversionCurrencyCode\": \"USD\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			// Execute
			_, err := uploader.Transform(job)
			require.Error(t, err)
			require.Contains(t, err.Error(), "adjustedConversionTime must be in ISO 8601")
		})

		t.Run("Transform() Test -> microsoftClickId is required(email and phone undefined) but not present", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"conversionValue\": \"100\", \"conversionCurrencyCode\": \"USD\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			// Execute
			_, err := uploader.Transform(job)
			expectedResult := fmt.Errorf("missing required field: microsoftClickId (or provide a hashed email/phone for enhanced conversions)")
			require.Equal(t, expectedResult.Error(), err.Error())
		})

		t.Run("Transform() Test -> microsoftClickId is required(email and phone empty) but not present", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": \"2023-05-22T06:27:54Z\", \"conversionValue\": \"100\", \"conversionCurrencyCode\": \"USD\",\n    \"email\":\"\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			// Execute
			_, err := uploader.Transform(job)
			expectedResult := fmt.Errorf("missing required field: microsoftClickId (or provide a hashed email/phone for enhanced conversions)")
			require.Equal(t, expectedResult.Error(), err.Error())
		})

		t.Run("Transform() Test -> successful even when microsoftClickId is missing as email/phone is present", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\n  \"type\": \"record\",\n  \"action\": \"insert\",\n  \"fields\": {\n    \"conversionName\": \"Test-Integration\",\n    \"conversionTime\": \"2023-05-22T18:27:54Z\",\n    \"adjustedConversionTime\": \"2023-05-22T18:27:54Z\",\n    \"conversionValue\": \"100\",\n    \"conversionCurrencyCode\": \"USD\",\n    \"email\": \"test@testmail.com\",\n    \"phone\": \"+911234567890\"\n  }\n}"),
			}
			uploader := &BingAdsBulkUploader{
				isHashRequired: true,
			}
			// insert does not reformat adjustedConversionTime (it is not validated on
			// insert), so it stays in its original ISO form; conversionTime is reformatted.
			expectedResp := `{"message":{"fields":{"adjustedConversionTime":"2023-05-22T18:27:54Z","conversionCurrencyCode":"USD","conversionName":"Test-Integration","conversionTime":"5/22/2023 6:27:54 PM","conversionValue":"100","email":"28a4da98f8812110001ab8ffacde3b38b4725a9e3570c39299fbf2d12c5aa70e","phone":"8c229df83de8ab269e90918846e326c4008c86481393223d17a30ff5a407b08e"},"action":"insert"},"metadata":{"jobId":0}}`
			// Execute
			resp, err := uploader.Transform(job)
			require.Equal(t, expectedResp, resp)
			require.NoError(t, err)
		})

		t.Run("Transform() Test -> null microsoftClickId falls back to email/phone and writes empty CSV cells", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				JobID: 42,
				EventPayload: []byte(`{
						"type": "record",
						"action": "insert",
						"fields": {
							"conversionName": "Test-Integration",
							"conversionTime": "2023-05-22T18:27:54Z",
							"conversionValue": "100",
							"conversionCurrencyCode": "USD",
							"microsoftClickId": null,
							"email": "test@testmail.com",
							"phone": "+911234567890",
							"externalAttributionCredit": null,
							"externalAttributionModel": "dummy goal name"
						}
					}`),
			}
			uploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", nil, false)

			resp, err := uploader.Transform(job)
			require.NoError(t, err)

			var data Data
			err = jsonrs.Unmarshal([]byte(resp), &data)
			require.NoError(t, err)

			var transformedFields map[string]string
			err = jsonrs.Unmarshal(data.Message.Fields, &transformedFields)
			require.NoError(t, err)
			require.Equal(t, "", transformedFields["microsoftClickId"])
			require.Equal(t, "", transformedFields["externalAttributionCredit"])

			actionFile, err := createActionFile("insert")
			require.NoError(t, err)
			defer os.Remove(actionFile.CSVFilePath)
			defer os.Remove(actionFile.ZipFilePath)

			err = uploader.populateZipFile(actionFile, resp, data)
			require.NoError(t, err)
			actionFile.CSVWriter.Flush()

			csvContent, err := os.ReadFile(actionFile.CSVFilePath)
			require.NoError(t, err)
			records, err := csv.NewReader(strings.NewReader(string(csvContent))).ReadAll()
			require.NoError(t, err)
			require.Len(t, records, 3)

			columnIndex := func(column string) int {
				for index, header := range records[0] {
					if header == column {
						return index
					}
				}
				t.Fatalf("missing CSV column %q", column)
				return -1
			}
			dataRow := records[2]
			require.Equal(t, "", dataRow[columnIndex("Microsoft Click Id")])
			require.Equal(t, "", dataRow[columnIndex("External Attribution Credit")])
			require.NotContains(t, dataRow, "<nil>")
		})

		t.Run("Transform() Test -> null microsoftClickId without email/phone is invalid", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte(`{
						"type": "record",
						"action": "insert",
						"fields": {
							"conversionName": "Test-Integration",
							"conversionTime": "2023-05-22T18:27:54Z",
							"conversionValue": "100",
							"conversionCurrencyCode": "USD",
							"microsoftClickId": null
						}
					}`),
			}
			uploader := &BingAdsBulkUploader{}

			_, err := uploader.Transform(job)
			require.Error(t, err)
			require.Equal(t, "missing required field: microsoftClickId (or provide a hashed email/phone for enhanced conversions)", err.Error())
		})

		t.Run("TestBingAdsTransformWithInvalidTimestamp", func(t *testing.T) {
			newBingAdsFixture(t)

			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", nil, true)

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

		t.Run("Transform() Test -> conversionTime is not a string", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\"type\": \"record\", \"action\": \"update\", \"fields\": {\"conversionName\": \"Test-Integration\", \"conversionTime\": 12345, \"conversionValue\": \"100\", \"microsoftClickId\": \"click_id\", \"conversionCurrencyCode\": \"USD\"}}"),
			}
			uploader := &BingAdsBulkUploader{}
			// Execute
			_, err := uploader.Transform(job)
			expectedResult := fmt.Errorf("conversionTime field is either not string or an empty string")
			require.Equal(t, expectedResult.Error(), err.Error())
		})

		t.Run("Transform() Test -> successful even when some fields are an integer", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\n  \"type\": \"record\",\n  \"action\": \"insert\",\n  \"fields\": {\n    \"conversionName\": \"Test-Integration\",\n    \"conversionTime\": \"2023-05-22T06:27:54Z\",\n    \"conversionValue\": 100,\n    \"microsoftClickId\": 54321,\n    \"conversionCurrencyCode\": \"USD\",\n    \"email\":\"test@testmail.com\",\n    \"phone\": 911234567890\n  }\n}"),
			}
			uploader := &BingAdsBulkUploader{
				isHashRequired: true,
			}
			expectedResp := `{"message":{"fields":{"conversionCurrencyCode":"USD","conversionName":"Test-Integration","conversionTime":"5/22/2023 6:27:54 AM","conversionValue":"100","email":"28a4da98f8812110001ab8ffacde3b38b4725a9e3570c39299fbf2d12c5aa70e","microsoftClickId":"54321","phone":"b7a259e2dd78c8492f72e2c61c60f965b1e2d796b2645de6b5f8109792fab686"},"action":"insert"},"metadata":{"jobId":0}}`
			// Execute
			resp, err := uploader.Transform(job)
			require.Equal(t, expectedResp, resp)
			require.NoError(t, err)
		})

		t.Run("Transform() Test -> successful with isHashRequired false", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\n  \"type\": \"record\",\n  \"action\": \"insert\",\n  \"fields\": {\n    \"conversionName\": \"Test-Integration\",\n    \"conversionTime\": \"2025-12-06T10:15:03+00:00\",\n    \"conversionValue\": \"15.942148760330578\",\n    \"microsoftClickId\": \"43d27c9a7f43100dcedc159df987176b\"\n  }\n}"),
			}
			uploader := &BingAdsBulkUploader{
				isHashRequired: false,
			}
			expectedResp := `{"message":{"fields":{"conversionName":"Test-Integration","conversionTime":"12/6/2025 10:15:03 AM","conversionValue":"15.942148760330578","microsoftClickId":"43d27c9a7f43100dcedc159df987176b"},"action":"insert"},"metadata":{"jobId":0}}`

			resp, err := uploader.Transform(job)
			require.NoError(t, err)
			require.Equal(t, expectedResp, resp)
		})

		t.Run("Transform() Test -> successful with email and phone when isHashRequired false", func(t *testing.T) {
			newBingAdsFixture(t)
			job := &jobsdb.JobT{
				EventPayload: []byte("{\n  \"type\": \"record\",\n  \"action\": \"insert\",\n  \"fields\": {\n    \"conversionName\": \"Test-Integration\",\n    \"conversionTime\": \"2025-12-06T10:15:03+00:00\",\n    \"conversionValue\": \"15.942148760330578\",\n    \"microsoftClickId\": \"43d27c9a7f43100dcedc159df987176b\",\n    \"email\": \"test@example.com\",\n    \"phone\": \"+1234567890\"\n  }\n}"),
			}
			uploader := &BingAdsBulkUploader{
				isHashRequired: false,
			}
			expectedResp := `{"message":{"fields":{"conversionName":"Test-Integration","conversionTime":"12/6/2025 10:15:03 AM","conversionValue":"15.942148760330578","email":"test@example.com","microsoftClickId":"43d27c9a7f43100dcedc159df987176b","phone":"+1234567890"},"action":"insert"},"metadata":{"jobId":0}}`

			resp, err := uploader.Transform(job)
			require.NoError(t, err)
			require.Equal(t, expectedResp, resp)
		})

		// populateZipFile writes one record into the action-specific CSV. Each case below
		// supplies the input fields and the FULL expected CSV (header + format version +
		// data row) so we compare the entire file, catching any column shift or drop.
		// The canonical field name emitted by rETL/VDM is `adjustedConversionTime`
		// (rudder-integrations-info/src/routes/bingads_offline_conversions/index.ts:46).
		t.Run("populateZipFile() -> writes the full expected CSV per action", func(t *testing.T) {
			testCases := []struct {
				name        string
				action      string
				fields      map[string]string
				expectedCSV [][]string
			}{
				{
					name:   "insert",
					action: "insert",
					fields: map[string]string{
						"conversionCurrencyCode":    "USD",
						"conversionName":            "Test-Integration",
						"conversionTime":            "4/19/2024 5:01:20 AM",
						"conversionValue":           "10",
						"microsoftClickId":          "click_id",
						"email":                     "hashed_email",
						"phone":                     "hashed_phone",
						"externalAttributionCredit": "0.5",
						"externalAttributionModel":  "dummy goal name",
					},
					expectedCSV: [][]string{
						{"Type", "Status", "Id", "Parent Id", "Client Id", "Name", "Conversion Currency Code", "Conversion Name", "Conversion Time", "Conversion Value", "Microsoft Click Id", "Hashed Email Address", "Hashed Phone Number", "External Attribution Credit", "External Attribution Model"},
						{"Format Version", "", "", "", "", "6.0", "", "", "", "", "", "", "", "", ""},
						{"Offline Conversion", "", "7", "", "", "", "USD", "Test-Integration", "4/19/2024 5:01:20 AM", "10", "click_id", "hashed_email", "hashed_phone", "0.5", "dummy goal name"},
					},
				},
				{
					name:   "update (Restate)",
					action: "update",
					fields: map[string]string{
						"conversionCurrencyCode": "USD",
						"conversionName":         "Test-Integration",
						"conversionTime":         "4/1/2024 9:23:54 AM",
						"conversionValue":        "100",
						"adjustedConversionTime": "4/1/2024 9:33:54 AM",
						"microsoftClickId":       "click_id",
						"email":                  "hashed_email",
						"phone":                  "hashed_phone",
					},
					expectedCSV: [][]string{
						{"Type", "Adjustment Type", "Client Id", "Id", "Name", "Conversion Name", "Conversion Time", "Adjustment Value", "Microsoft Click Id", "Hashed Email Address", "Hashed Phone Number", "Adjusted Currency Code", "Adjustment Time"},
						{"Format Version", "", "", "", "6.0", "", "", "", "", "", "", "", ""},
						{"Offline Conversion", "Restate", "", "7", "", "Test-Integration", "4/1/2024 9:23:54 AM", "100", "click_id", "hashed_email", "hashed_phone", "USD", "4/1/2024 9:33:54 AM"},
					},
				},
				{
					name:   "delete (Retract)",
					action: "delete",
					fields: map[string]string{
						"conversionName":         "Test-Integration",
						"conversionTime":         "4/1/2024 9:23:54 AM",
						"adjustedConversionTime": "4/1/2024 9:33:54 AM",
						"microsoftClickId":       "click_id",
						"email":                  "hashed_email",
						"phone":                  "hashed_phone",
					},
					expectedCSV: [][]string{
						{"Type", "Adjustment Type", "Client Id", "Id", "Name", "Conversion Name", "Conversion Time", "Microsoft Click Id", "Hashed Email Address", "Hashed Phone Number", "Adjustment Time"},
						{"Format Version", "", "", "", "6.0", "", "", "", "", "", ""},
						{"Offline Conversion", "Retract", "", "7", "", "Test-Integration", "4/1/2024 9:23:54 AM", "click_id", "hashed_email", "hashed_phone", "4/1/2024 9:33:54 AM"},
					},
				},
			}
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					newBingAdsFixture(t)
					bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", nil, false)

					actionFile, err := createActionFile(tc.action)
					require.NoError(t, err)
					defer os.Remove(actionFile.CSVFilePath)
					defer os.Remove(actionFile.ZipFilePath)

					fieldsJSON, err := jsonrs.Marshal(tc.fields)
					require.NoError(t, err)

					err = bulkUploader.populateZipFile(actionFile, "some line", Data{
						Message:  Message{Action: tc.action, Fields: fieldsJSON},
						Metadata: Metadata{JobID: 7},
					})
					require.NoError(t, err)
					actionFile.CSVWriter.Flush()

					csvContent, err := os.ReadFile(actionFile.CSVFilePath)
					require.NoError(t, err)
					records, err := csv.NewReader(strings.NewReader(string(csvContent))).ReadAll()
					require.NoError(t, err)

					// Full comparison of every row (header, format version, data row).
					require.Equal(t, tc.expectedCSV, records)
				})
			}
		})
	})
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

func ZipCSVFile(csvFilePath, zipFilePath string) error {
	// Create a new ZIP file
	zipFile, err := os.Create(zipFilePath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	// Create a new zip writer
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// Extract the base name (file name) of the CSV file for the zip entry
	csvFileName := filepath.Base(csvFilePath)

	// Create a new zip entry for the CSV file
	zipEntryWriter, err := zipWriter.Create(csvFileName)
	if err != nil {
		return err
	}

	// Open the CSV file to read its contents
	csvFile, err := os.Open(csvFilePath)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	// Copy the contents of the CSV file into the zip entry
	if _, err := io.Copy(zipEntryWriter, csvFile); err != nil {
		return err
	}

	// Closing the zip writer ensures all data is flushed to zipFile
	return zipWriter.Close()
}
