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

type bingAdsFixture struct {
	ctrl           *gomock.Controller
	bingAdsService *mockbulkservice.MockBulkServiceI
}

func newBingAdsFixture(t *testing.T) *bingAdsFixture {
	t.Helper()
	config.Reset()
	config.Set("BatchRouter.BING_ADS.MaxUploadLimit", 1*bytesize.KB)
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
	require.NoError(t, os.Mkdir(filepath.Join(dir, "rudder-async-destination-logs"), 0o755))
	t.Setenv("RUDDER_TMPDIR", dir)
}

func TestBingAdsAudience(t *testing.T) {
	t.Run("Bing ads", func(t *testing.T) {
		t.Run("TestBingAdsUploadPartialSuccessCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload1",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload2",
				RequestId: misc.FastUUID().URN(),
			}, nil)

			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload1", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
				TrackingId: "randomTrackingId1",
				RequestId:  "randomRequestId1",
			}, nil)
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))

			setupRudderTmpDir(t)

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

			// making upload function call
			received := bulkUploader.Upload(t.Context(), &asyncDestination)
			received.ImportingParameters = json.RawMessage{}

			require.Equal(t, expected, received)
		})

		t.Run("TestBingAdsUploadFailedGetBulkUploadUrl", func(t *testing.T) {
			f := newBingAdsFixture(t)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))

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
			}
			setupRudderTmpDir(t)
			received := bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})

		t.Run("TestBingAdsUploadEmptyGetBulkUploadUrl", func(t *testing.T) {
			f := newBingAdsFixture(t)
			ClientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &ClientI)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))

			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))

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
			}

			setupRudderTmpDir(t)
			received := bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})

		t.Run("TestBingAdsUploadFailedUploadBulkFile", func(t *testing.T) {
			f := newBingAdsFixture(t)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload1",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload2",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload1", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(nil, fmt.Errorf("Error in uploading bulk file"))

			setupRudderTmpDir(t)

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
			}
			received := bulkUploader.Upload(t.Context(), &asyncDestination)

			require.Equal(t, expected, received)
		})

		t.Run("TestBingAdsPollSuccessCase", func(t *testing.T) {
			f := newBingAdsFixture(t)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
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
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)

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
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
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
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
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
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
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
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId456").Return(&bingads_sdk.GetBulkUploadStatusResponse{
				PercentComplete: int64(100),
				RequestStatus:   "Completed",
				ResultFileUrl:   "",
			}, nil)

			f.bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
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
			client := ts.Client()
			modifiedURL := ts.URL // Use the test server URL
			clientI := Client{client: client, URL: modifiedURL}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)

			UploadStatsInput := common.GetUploadStatsInput{
				FailedJobParameters: modifiedURL,
				ImportingList: []*jobsdb.JobT{
					{
						JobID: 5,
					},
					{
						JobID: 6,
					},
					{
						JobID: 7,
					},
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
			recievedResponse := bulkUploader.GetUploadStats(UploadStatsInput)
			require.Equal(t, expectedResp, recievedResponse)
		})

		t.Run("TestBingAdsGetUploadStats for wrong audience Id", func(t *testing.T) {
			f := newBingAdsFixture(t)
			csvPath := "testdata/BulkUpload-02-28-2024-c7a38716-4d65-44a7-bf28-8879ab9b1da0-Results.csv"
			zipPath := "testdata/BulkUpload-02-28-2024-c7a38716-4d65-44a7-bf28-8879ab9b1da0-Results.zip"
			err := ZipCSVFile(csvPath, zipPath)
			require.NoError(t, err)
			errorsTemplateFilePath := filepath.Join(currentDir, "testdata/BulkUpload-02-28-2024-c7a38716-4d65-44a7-bf28-8879ab9b1da0-Results.zip") // Path of the source file
			// Create a test server with a custom handler function
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Set the appropriate headers for a zip file response
				w.Header().Set("Content-Type", "application/zip")
				w.Header().Set("Content-Disposition", "attachment; filename='status-wrong-audience.zip'")
				http.ServeFile(w, r, errorsTemplateFilePath)
			}))
			defer ts.Close()
			client := ts.Client()
			modifiedURL := ts.URL // Use the test server URL
			clientI := Client{client: client, URL: modifiedURL}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)

			UploadStatsInput := common.GetUploadStatsInput{
				FailedJobParameters: modifiedURL,
				ImportingList: []*jobsdb.JobT{
					{
						JobID: 1,
					},
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
			recievedResponse := bulkUploader.GetUploadStats(UploadStatsInput)
			os.Remove(zipPath)
			require.Equal(t, expectedResp, recievedResponse)
		})

		t.Run("TestNewManagerInternal", func(t *testing.T) {
			f := newBingAdsFixture(t)
			oauthV2Service := MockAuthorizer.NewMockOAuthHandler(f.ctrl)
			// oauthService := mocksoauthservice.NewMockOauthHandler(ctrl)
			oauthV2Service.EXPECT().FetchToken(gomock.Any()).Return(json.RawMessage(`
							{
							"AccessToken": "dummyacesstoken",
							"RefreshToken": "dummyRefreshToken",
							"Developer_token": "dummyDeveloperToken",
							"ExpirationDate": "2023-01-31T23:59:59.999Z"
							}`), nil)

			bingAdsUploader, err := newManagerInternal(logger.NOP, stats.NOP, &destination, oauthV2Service)
			require.NoError(t, err)
			require.NotNil(t, bingAdsUploader)
		})

		t.Run("TestBingAdsUploadNoTrackingId", func(t *testing.T) {
			f := newBingAdsFixture(t)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", f.bingAdsService, &clientI)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(nil, fmt.Errorf("unable to upload bulk file, check your credentials"))
			f.bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(nil, fmt.Errorf("unable to upload bulk file, check your credentials"))

			setupRudderTmpDir(t)

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
			}

			// making upload function call
			received := bulkUploader.Upload(t.Context(), &asyncDestination)
			received.ImportingParameters = json.RawMessage{}

			require.Equal(t, expected, received)
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
