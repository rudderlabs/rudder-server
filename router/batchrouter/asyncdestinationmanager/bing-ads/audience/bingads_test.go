package audience

import (
	"archive/zip"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/stats"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	bingads_sdk "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	mockbulkservice "github.com/rudderlabs/bing-ads-go-sdk/mocks"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksoauthservice "github.com/rudderlabs/rudder-server/mocks/services/oauth"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/services/oauth"
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

var _ = Describe("Bing ads Audience", func() {
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
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)
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
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}
			expected := common.AsyncUploadOutput{
				FailedReason:        "{\"error\":\"Add:error in uploading the bulk file: unable to get bulk upload url, check your credentials\"}",
				ImportingJobIDs:     []int64{3, 4},
				FailedJobIDs:        []int64{1, 2},
				ImportingParameters: stdjson.RawMessage{},
				ImportingCount:      2,
				FailedCount:         2,
			}

			// making upload function call
			received := bulkUploader.Upload(&asyncDestination)
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
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)
			bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))
			bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("Error in getting bulk upload url"))

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}
			var parameters common.ImportParameters
			parameters.ImportId = ""
			importParameters, err := stdjson.Marshal(parameters)
			if err != nil {
				fmt.Printf("Failed to unmarshal parameters: %v\n", err)
				return
			}
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{3, 4, 1, 2},
				FailedReason:        "{\"error\":\"Remove:error in getting bulk upload url: Error in getting bulk upload url,Add:error in getting bulk upload url: Error in getting bulk upload url\"}",
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
			received := bulkUploader.Upload(&asyncDestination)
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
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			ClientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &ClientI)
			bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))

			bingAdsService.EXPECT().GetBulkUploadUrl().Return(nil, fmt.Errorf("unable to get bulk upload url, check your credentials"))

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}
			var parameters common.ImportParameters
			parameters.ImportId = ""
			importParameters, err := stdjson.Marshal(parameters)
			if err != nil {
				fmt.Printf("Failed to remove the temporary directory: %v\n", err)
				return
			}
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{3, 4, 1, 2},
				FailedReason:        "{\"error\":\"Remove:error in getting bulk upload url: unable to get bulk upload url, check your credentials,Add:error in getting bulk upload url: unable to get bulk upload url, check your credentials\"}",
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
			received := bulkUploader.Upload(&asyncDestination)
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
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)
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
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}
			var parameters common.ImportParameters
			parameters.ImportId = ""
			importParameters, err := stdjson.Marshal(parameters)
			if err != nil {
				fmt.Printf("Failed to remove the temporary directory: %v\n", err)
				return
			}
			expected := common.AsyncUploadOutput{
				FailedJobIDs:        []int64{3, 4, 1, 2},
				FailedReason:        "{\"error\":\"Remove:error in uploading the bulk file: Error in uploading bulk file,Add:error in uploading the bulk file: Error in uploading bulk file\"}",
				FailedCount:         4,
				DestinationID:       destination.ID,
				ImportingParameters: stdjson.RawMessage(importParameters),
			}
			received := bulkUploader.Upload(&asyncDestination)

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
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)

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
			recievedResponse := bulkUploader.Poll(pollInput)
			Expect(recievedResponse).To(Equal(expectedResp))
		})

		It("TestBingAdsPollFailureCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)

			bingAdsService.EXPECT().GetBulkUploadStatus("dummyRequestId123").Return(nil, fmt.Errorf("failed to get bulk upload status:"))
			pollInput := common.AsyncPoll{
				ImportId: "dummyRequestId123",
			}
			expectedResp := common.PollStatusResponse{
				StatusCode: 500,
				HasFailed:  true,
			}
			recievedResponse := bulkUploader.Poll(pollInput)
			Expect(recievedResponse).To(Equal(expectedResp))
		})

		It("TestBingAdsPollPartialFailureCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)

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
			recievedResponse := bulkUploader.Poll(pollInput)

			os.Remove(expectedResp.FailedJobParameters)

			Expect(recievedResponse).To(Equal(expectedResp))
		})

		It("TestBingAdsPollPendingStatusCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)

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
			recievedResponse := bulkUploader.Poll(pollInput)

			os.Remove(expectedResp.FailedJobParameters)

			Expect(recievedResponse).To(Equal(expectedResp))
		})

		It("TestBingAdsPollFailedStatusCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)

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
			recievedResponse := bulkUploader.Poll(pollInput)

			os.Remove(expectedResp.FailedJobParameters)

			Expect(recievedResponse).To(Equal(expectedResp))
		})

		It("TestBingAdsPollSuccessAndFailedStatusCase", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)

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
				FailedJobParameters: ",", // empty file
			}
			recievedResponse := bulkUploader.Poll(pollInput)

			os.Remove(expectedResp.FailedJobParameters)

			Expect(recievedResponse).To(Equal(expectedResp))
		})

		It("TestBingAdsGetUploadStats", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
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
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)

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
			Expect(recievedResponse).To(Equal(expectedResp))
		})

		It("TestBingAdsGetUploadStats for wrong audience Id", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			csvPath := "testdata/BulkUpload-02-28-2024-c7a38716-4d65-44a7-bf28-8879ab9b1da0-Results.csv"
			zipPath := "testdata/BulkUpload-02-28-2024-c7a38716-4d65-44a7-bf28-8879ab9b1da0-Results.zip"
			err := ZipCSVFile(csvPath, zipPath)
			Expect(err).To(BeNil())
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
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
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)

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
			Expect(recievedResponse).To(Equal(expectedResp))
		})

		It("TestNewManagerInternal", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			oauthService := mocksoauthservice.NewMockAuthorizer(ctrl)
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

			bingAdsUploader, err := newManagerInternal(logger.NOP, stats.NOP, &destination, oauthService, nil)
			Expect(err).To(BeNil())
			Expect(bingAdsUploader).ToNot(BeNil())
		})

		It("TestBingAdsUploadNoTrackingId", func() {
			initBingads()
			ctrl := gomock.NewController(GinkgoT())
			bingAdsService := mockbulkservice.NewMockBulkServiceI(ctrl)
			clientI := Client{}
			bulkUploader := NewBingAdsBulkUploader(logger.NOP, stats.NOP, "BING_ADS", bingAdsService, &clientI)
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
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}
			expected := common.AsyncUploadOutput{
				FailedReason:        "{\"error\":\"Remove:error in uploading the bulk file: unable to upload bulk file, check your credentials,Add:error in uploading the bulk file: unable to upload bulk file, check your credentials\"}",
				FailedJobIDs:        []int64{3, 4, 1, 2},
				ImportingParameters: stdjson.RawMessage{},
				ImportingCount:      0,
				FailedCount:         4,
			}

			// making upload function call
			received := bulkUploader.Upload(&asyncDestination)
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
