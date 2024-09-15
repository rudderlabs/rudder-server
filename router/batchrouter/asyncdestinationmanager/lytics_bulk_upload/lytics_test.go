package lyticsBulkUpload_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/stats"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	mocks "github.com/rudderlabs/rudder-server/mocks/router/lytics_bulk_upload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	lyticsBulkUpload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/lytics_bulk_upload"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	once        sync.Once
	destination = backendconfig.DestinationT{
		Name: "LYTICS_BULK_UPLOAD",
		Config: map[string]interface{}{
			"lyticsAccountId":  "1234",
			"lyticsApiKey":     "1234567",
			"lyticsStreamName": "test",
			"timestampField":   "timestamp",
			"streamTraitsMapping": []map[string]string{
				{
					"rudderProperty": "name",
					"lyticsProperty": "name",
				},
			},
		},
		WorkspaceID: "workspace_id",
	}
	currentDir, _ = os.Getwd()
)

func initLytics() {
	once.Do(func() {
		logger.Reset()
		misc.Init()
	})
}

var _ = Describe("LYTICS_BULK_UPLOAD test", func() {
	Context("When uploading the file", func() {
		BeforeEach(func() {
			config.Reset()
			config.Set("BatchRouter.LYTICS_BULK_UPLOAD.MaxUploadLimit", 1*bytesize.MB)
		})

		AfterEach(func() {
			config.Reset()
		})
		It("TestLyticsUploadWrongFilepath", func() {
			initLytics()
			ctrl := gomock.NewController(GinkgoT())
			lyticsService := mocks.NewMockLyticsService(ctrl)
			LyticsServiceImpl := lyticsBulkUpload.LyticsServiceImpl{
				BulkApi: "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}
			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader(logger.NOP, stats.NOP, "LYTICS_BULK_UPLOAD", destination.Config["lyticsApiKey"].(string), LyticsServiceImpl.BulkApi, lyticsService)}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        "",
				Destination:     &destination,
				Manager:         bulkUploader,
			}
			expected := common.AsyncUploadOutput{
				FailedReason:        "got error while transforming the file. failed to open existing file: open : no such file or directory",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1, 2, 3, 4},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         4,
			}
			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})

		It("TestEloquaErrorWhileUploadingData", func() {
			initLytics()
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			// Mock the LyticsService
			lyticsService := mocks.NewMockLyticsService(ctrl)

			// Set up the LyticsServiceImpl with the BulkApi endpoint
			LyticsServiceImpl := lyticsBulkUpload.LyticsServiceImpl{
				BulkApi: "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}

			// Set up the SimpleAsyncDestinationManager with the LyticsBulkUploader
			bulkUploader := common.SimpleAsyncDestinationManager{
				UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader(logger.NOP, stats.NOP, "LYTICS_BULK_UPLOAD", destination.Config["lyticsApiKey"].(string), LyticsServiceImpl.BulkApi, lyticsService),
			}

			// Set up the AsyncDestinationStruct with test data
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}

			// Define the expected HttpRequestData
			expectedData := &lyticsBulkUpload.HttpRequestData{
				Authorization: "1234567",
				Endpoint:      "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}

			// Set up mock expectations for UploadBulkFile
			lyticsService.EXPECT().
				UploadBulkFile(gomock.Eq(expectedData), gomock.Any()).
				Return(fmt.Errorf("Upload failed with status code: 400")).
				Times(1)

			// Define the expected AsyncUploadOutput
			expected := common.AsyncUploadOutput{
				FailedReason:        "error in uploading the bulk file: Upload failed with status code: 400",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1, 2, 3},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         3,
			}

			// Call the Upload method and capture the result
			received := bulkUploader.Upload(&asyncDestination)

			// Assert that the received output matches the expected output
			Expect(received).To(Equal(expected))
		})

		It("TestEloquaErrorWhileUploadingData", func() {
			initLytics()
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			// Mock the LyticsService
			lyticsService := mocks.NewMockLyticsService(ctrl)

			// Set up the LyticsServiceImpl with the BulkApi endpoint
			LyticsServiceImpl := lyticsBulkUpload.LyticsServiceImpl{
				BulkApi: "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}

			// Set up the SimpleAsyncDestinationManager with the LyticsBulkUploader
			bulkUploader := common.SimpleAsyncDestinationManager{
				UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader(logger.NOP, stats.NOP, "LYTICS_BULK_UPLOAD", destination.Config["lyticsApiKey"].(string), LyticsServiceImpl.BulkApi, lyticsService),
			}

			// Set up the AsyncDestinationStruct with test data
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}

			// Define the expected HttpRequestData
			expectedData := &lyticsBulkUpload.HttpRequestData{
				Authorization: "1234567",
				Endpoint:      "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}

			// Set up mock expectations for UploadBulkFile
			lyticsService.EXPECT().
				UploadBulkFile(gomock.Eq(expectedData), gomock.Any()).
				Return(nil).
				Times(1)

			// Define the expected AsyncUploadOutput
			expected := common.AsyncUploadOutput{
				ImportingJobIDs:     []int64{1, 2, 3},
				FailedJobIDs:        []int64{},
				ImportingParameters: nil,
				ImportingCount:      3,
				FailedCount:         0,
			}

			// Call the Upload method and capture the result
			received := bulkUploader.Upload(&asyncDestination)

			// Assert that the received output matches the expected output
			Expect(received).To(Equal(expected))
		})
	})
})
