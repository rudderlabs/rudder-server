package bingads

import (
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	bingads_sdk "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/bingads"
	mocks_oauth "github.com/rudderlabs/rudder-server/mocks/services/oauth"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
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

var _ = Describe("Bing ads", func() {
	Context("Bing ads", func() {
		BeforeEach(func() {
			config.Reset()
			config.Set("BatchRouter.BING_ADS.MaxUploadLimit", 1*bytesize.KB)
		})

		AfterEach(func() {
			config.Reset()
		})

		It("TestBingAdsUploadPartialSuccessCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)
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
			bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload2", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
				TrackingId: "",
				RequestId:  "",
			}, nil)

			dir, err := os.MkdirTemp("/tmp", "rudder-server")
			if err != nil {
				fmt.Printf("Failed to create temporary directory: %v\n", err)
				return
			}

			subDir := filepath.Join(dir, "rudder-async-destination-logs")
			err = os.Mkdir(subDir, 0o755)
			if err != nil {
				fmt.Printf("Failed to create the directory 'something': %v\n", err)
				return
			}

			Expect(err).To(BeNil())
			GinkgoT().Setenv("RUDDER_TMPDIR", dir)

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
			}
			expected := common.AsyncUploadOutput{
				FailedReason:        "{\"error\":\"1:getting empty string in upload url or request id,\"}",
				ImportingJobIDs:     []int64{1, 2},
				FailedJobIDs:        []int64{3, 4},
				ImportingParameters: stdjson.RawMessage{},
				ImportingCount:      2,
				FailedCount:         2,
			}

			// making upload function call
			received := bulkUploader.Upload(&destination, &asyncDestination)
			received.ImportingParameters = stdjson.RawMessage{}

			// Remove the directory and its contents
			err = os.RemoveAll(dir)
			if err != nil {
				fmt.Printf("Failed to remove the temporary directory: %v\n", err)
				return
			}

			Expect(received).To(Equal(expected))
		})

		It("TestBingAdsUploadFailedGetBulkUploadUrl", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)
			bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))
			bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
			}
			var parameters common.ImportParameters
			parameters.ImportId = ""
			importParameters, _ := json.Marshal(parameters)
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{1, 2, 3, 4},
				FailedReason:        "{\"error\":\"0:error in getting bulk upload url: Error in getting bulk upload url,1:error in getting bulk upload url: Error in getting bulk upload url,\"}",
				ImportingCount:      0,
				FailedCount:         4,
				AbortCount:          0,
				ImportingParameters: stdjson.RawMessage(importParameters),
			}
			dir, err := os.MkdirTemp("/tmp", "rudder-server")
			if err != nil {
				fmt.Printf("Failed to create temporary directory: %v\n", err)
				return
			}

			subDir := filepath.Join(dir, "rudder-async-destination-logs")
			err = os.Mkdir(subDir, 0o755)
			if err != nil {
				fmt.Printf("Failed to create the directory 'something': %v\n", err)
				return
			}
			GinkgoT().Setenv("RUDDER_TMPDIR", dir)
			received := bulkUploader.Upload(&destination, &asyncDestination)
			err = os.RemoveAll(dir)
			if err != nil {
				fmt.Printf("Failed to remove the temporary directory: %v\n", err)
				return
			}
			Expect(received).To(Equal(expected))
		})

		It("TestBingAdsUploadEmptyGetBulkUploadUrl", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
			ClientI := Client{}
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &ClientI)
			bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "",
				RequestId: "",
			}, nil)

			bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "",
				RequestId: "",
			}, nil)

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
			}
			var parameters common.ImportParameters
			parameters.ImportId = ""
			importParameters, _ := json.Marshal(parameters)
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{1, 2, 3, 4},
				FailedReason:        "{\"error\":\"0:getting empty string in upload url or request id,1:getting empty string in upload url or request id,\"}",
				FailedCount:         4,
				DestinationID:       destination.ID,
				ImportingParameters: stdjson.RawMessage(importParameters),
			}

			dir, err := os.MkdirTemp("/tmp", "rudder-server")
			if err != nil {
				fmt.Printf("Failed to create temporary directory: %v\n", err)
				return
			}

			subDir := filepath.Join(dir, "rudder-async-destination-logs")
			err = os.Mkdir(subDir, 0o755)
			if err != nil {
				fmt.Printf("Failed to create the directory 'something': %v\n", err)
				return
			}
			GinkgoT().Setenv("RUDDER_TMPDIR", dir)
			received := bulkUploader.Upload(&destination, &asyncDestination)
			err = os.RemoveAll(dir)
			if err != nil {
				fmt.Printf("Failed to remove the temporary directory: %v\n", err)
				return
			}
			Expect(received).To(Equal(expected))
		})

		It("TestBingAdsUploadFailedUploadBulkFile", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)
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
			if err != nil {
				fmt.Printf("Failed to create temporary directory: %v\n", err)
				return
			}

			subDir := filepath.Join(dir, "rudder-async-destination-logs")
			err = os.Mkdir(subDir, 0o755)
			if err != nil {
				fmt.Printf("Failed to create the directory 'something': %v\n", err)
				return
			}

			Expect(err).To(BeNil())
			GinkgoT().Setenv("RUDDER_TMPDIR", dir)

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
			}
			var parameters common.ImportParameters
			parameters.ImportId = ""
			importParameters, _ := json.Marshal(parameters)
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{1, 2, 3, 4},
				FailedReason:        "{\"error\":\"0:error in uploading the bulk file: Error in uploading bulk file,1:error in uploading the bulk file: Error in uploading bulk file,\"}",
				FailedCount:         4,
				DestinationID:       destination.ID,
				ImportingParameters: stdjson.RawMessage(importParameters),
			}
			received := bulkUploader.Upload(&destination, &asyncDestination)

			// Remove the directory and its contents
			err = os.RemoveAll(dir)
			if err != nil {
				fmt.Printf("Failed to remove the temporary directory: %v\n", err)
				return
			}
			Expect(received).To(Equal(expected))
		})

		It("TestBingAdsPollSuccessCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)

			bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
				PercentComplete: int64(100),
				RequestStatus:   "Completed",
				ResultFileUrl:   "http://dummyurl.com",
			}, nil)
			pollInput := common.AsyncPoll{
				ImportId: "dummyRequestId123",
			}
			expectedResp := common.PollStatusResponse{
				Complete:       true,
				StatusCode:     200,
				HasFailed:      false,
				HasWarning:     false,
				FailedJobsURL:  "",
				WarningJobsURL: "",
				OutputFilePath: ",",
			}
			expectedStatus := 200
			recievedResponse, RecievedStatus := bulkUploader.Poll(pollInput)

			Expect(recievedResponse).To(Equal(expectedResp))
			Expect(RecievedStatus).To(Equal(expectedStatus))
		})

		It("TestBingAdsPollFailureCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)

			bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(nil, fmt.Errorf("failed to get bulk upload status:"))
			pollInput := common.AsyncPoll{
				ImportId: "dummyRequestId123",
			}
			expectedResp := common.PollStatusResponse{
				Complete:       false,
				StatusCode:     400,
				HasFailed:      true,
				HasWarning:     false,
				FailedJobsURL:  "",
				WarningJobsURL: "",
				OutputFilePath: "",
			}
			expectedStatus := 500
			recievedResponse, RecievedStatus := bulkUploader.Poll(pollInput)

			Expect(recievedResponse).To(Equal(expectedResp))
			Expect(RecievedStatus).To(Equal(expectedStatus))
		})

		It("TestBingAdsPollPartialFailureCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
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
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)

			bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
				PercentComplete: int64(100),
				RequestStatus:   "CompletedWithErrors",
				ResultFileUrl:   ts.URL,
			}, nil)
			pollInput := common.AsyncPoll{
				ImportId: "dummyRequestId123",
			}

			expectedResp := common.PollStatusResponse{
				Complete:       true,
				StatusCode:     200,
				HasFailed:      true,
				HasWarning:     false,
				FailedJobsURL:  "",
				WarningJobsURL: "",
				OutputFilePath: "/tmp/BulkUpload-05-31-2023-6326c4f9-0745-4c43-8126-621b4a1849ad-Results.csv,",
			}
			expectedStatus := 200
			recievedResponse, RecievedStatus := bulkUploader.Poll(pollInput)

			os.Remove(expectedResp.OutputFilePath)

			Expect(recievedResponse).To(Equal(expectedResp))
			Expect(RecievedStatus).To(Equal(expectedStatus))
		})

		It("TestBingAdsPollPartialFailureCaseWithWrongFilePath", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
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
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)

			bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(&bingads_sdk.GetBulkUploadStatusResponse{
				PercentComplete: int64(100),
				RequestStatus:   "CompletedWithErrors",
				ResultFileUrl:   ts.URL,
			}, nil)
			pollInput := common.AsyncPoll{
				ImportId: "dummyRequestId123",
			}

			expectedResp := common.PollStatusResponse{
				Complete:       false,
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

			Expect(recievedResponse).To(Equal(expectedResp))
			Expect(RecievedStatus).To(Equal(expectedStatus))
		})

		It("TestBingAdsGetUploadStats", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
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
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)

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
			Expect(recievedResponse).To(Equal(expectedResp))
			Expect(RecievedStatus).To(Equal(expectedStatus))
		})

		It("TestBingAdsUploadFailedWhileTransformingFile", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)
			dir, err := os.MkdirTemp("/tmp", "rudder-server")
			if err != nil {
				fmt.Printf("Failed to create temporary directory: %v\n", err)
				return
			}

			subDir := filepath.Join(dir, "rudder-async-destination-logs")
			err = os.Mkdir(subDir, 0o755)
			if err != nil {
				fmt.Printf("Failed to create the directory 'something': %v\n", err)
				return
			}

			Expect(err).To(BeNil())
			GinkgoT().Setenv("RUDDER_TMPDIR", dir)

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
			received := bulkUploader.Upload(&destination, &asyncDestination)

			// Remove the directory and its contents
			err = os.RemoveAll(dir)
			if err != nil {
				fmt.Printf("Failed to remove the temporary directory: %v\n", err)
				return
			}
			Expect(received).To(Equal(expected))
		})

		It("TestNewManagerInternal", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
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

			bingAdsUploader, err := newManagerInternal(&destination, oauthService)
			Expect(err).To(BeNil())
			Expect(bingAdsUploader).ToNot(BeNil())
		})

		It("TestBingAdsUploadNoTrackingId", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mock_bulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader("BING_ADS", bingAdsService, &clientI)
			bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			bingAdsService.EXPECT().GetBulkUploadUrl().Return(&bingads_sdk.GetBulkUploadUrlResponse{
				UploadUrl: "http://localhost/upload",
				RequestId: misc.FastUUID().URN(),
			}, nil)
			bingAdsService.EXPECT().UploadBulkFile("http://localhost/upload", gomock.Any()).Return(&bingads_sdk.UploadBulkFileResponse{
				TrackingId: "",
				RequestId:  "",
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
			err = os.Mkdir(subDir, 0o755)
			if err != nil {
				fmt.Printf("Failed to create the directory 'something': %v\n", err)
				return
			}

			Expect(err).To(BeNil())
			GinkgoT().Setenv("RUDDER_TMPDIR", dir)

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "test-files/uploadData.txt"),
			}
			expected := common.AsyncUploadOutput{
				FailedReason:        "{\"error\":\"0:getting empty string in upload url or request id,1:getting empty string in upload url or request id,\"}",
				FailedJobIDs:        []int64{1, 2, 3, 4},
				ImportingParameters: stdjson.RawMessage{},
				ImportingCount:      0,
				FailedCount:         4,
			}

			// making upload function call
			received := bulkUploader.Upload(&destination, &asyncDestination)
			received.ImportingParameters = stdjson.RawMessage{}

			// Remove the directory and its contents
			err = os.RemoveAll(dir)
			if err != nil {
				fmt.Printf("Failed to remove the temporary directory: %v\n", err)
				return
			}

			Expect(received).To(Equal(expected))
		})
	})
})

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
