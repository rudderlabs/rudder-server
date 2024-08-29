package lyticsBulkUpload_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/mock/gomock"

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
			config.Set("BatchRouter.LYTICS_BULK_UPLOAD.MaxUploadLimit", 200*bytesize.B)
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
			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader("LYTICS_BULK_UPLOAD", "abcd", LyticsServiceImpl.BulkApi, lyticsService)}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        "",
				Destination:     &destination,
				Manager:         bulkUploader,
			}
			expected := common.AsyncUploadOutput{
				FailedReason:        "got error while opening the file. open : no such file or directory",
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
			lyticsService := mocks.NewMockLyticsService(ctrl)
			LyticsServiceImpl := lyticsBulkUpload.LyticsServiceImpl{
				BulkApi: "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}
			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader("LYTICS_BULK_UPLOAD", "abcd", LyticsServiceImpl.BulkApi, lyticsService)}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}

			lyticsService.EXPECT().UploadBulkFile(gomock.Any(), asyncDestination.FileName).Return(fmt.Errorf("Upload failed with status code 400"))

			expected := common.AsyncUploadOutput{
				FailedReason:        "error in uploading the bulk file: Upload failed with status code 400",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1, 2, 3, 4},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         4,
			}
			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})

		It("TestEloquaSuccessfulIdentify", func() {
			initLytics()
			ctrl := gomock.NewController(GinkgoT())
			lyticsService := mocks.NewMockLyticsService(ctrl)
			LyticsServiceImpl := lyticsBulkUpload.LyticsServiceImpl{
				BulkApi: "https://api.example.com",
			}
			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader("LYTICS_BULK_UPLOAD", "abcd", LyticsServiceImpl.BulkApi, lyticsService)}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
				Manager:         bulkUploader,
			}

			lyticsService.EXPECT().UploadBulkFile(gomock.Any(), asyncDestination.FileName).Return(nil)

			expected := common.AsyncUploadOutput{
				FailedReason:    "failed as the fileSizeLimit has over",
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    nil,
				ImportingCount:  4,
				FailedCount:     0,
			}
			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})
	})
})
