package clevertapSegment_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/mock/gomock"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	mocks "github.com/rudderlabs/rudder-server/mocks/router/clevertap_segment"
	ClevertapSegment "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/clevertap_segment"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	once        sync.Once
	destination = backendconfig.DestinationT{
		Name: "CLEVERTAP_SEGMENT",
		Config: map[string]interface{}{
			"clevertapAccountId":  "1234",
			"clevertapAccountKey": "1234567",
		},
		WorkspaceID: "workspace_id",
	}
	currentDir, _ = os.Getwd()
)

func initClevertap() {
	once.Do(func() {
		logger.Reset()
		misc.Init()
	})
}

var _ = Describe("CLEVERTAP_SEGMENT test", func() {
	Context("When uploading the file", func() {
		BeforeEach(func() {
			config.Reset()
			config.Set("BatchRouter.CLEVERTAP_SEGMENT.MaxUploadLimit", 1*bytesize.MB)

			// Ensure the log directory exists
			logDir := "/tmp/rudder-async-destination-logs"
			err := os.MkdirAll(logDir, os.ModePerm) // Create the directory if it doesn't exist
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			config.Reset()
		})

		It("TestClevertapUploadWrongFilepath", func() {
			initClevertap()
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			clevertapService := mocks.NewMockClevertapService(ctrl)
			clevertapServiceImpl := ClevertapSegment.ClevertapServiceImpl{
				BulkApi:   "https://api.clevertap.com/get_custom_list_segment_url",
				NotifyApi: "https://api.clevertap.com/upload_custom_list_segment_completed",
				ConnectionConfig: &ClevertapSegment.ConnectionConfig{
					SourceId:      "source123",
					DestinationId: "destination456",
					Enabled:       true,
					Config: ClevertapSegment.ConnConfig{
						Destination: ClevertapSegment.Destination{
							SchemaVersion: "v1.0",
							SegmentName:   "User Segment A",
							AdminEmail:    "admin@example.com",
							SenderName:    "Rudderstack",
						},
					},
				},
			}
			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: ClevertapSegment.NewClevertapBulkUploader(logger.NOP, stats.NOP, "CLEVERTAP_SEGMENT", destination.Config["clevertapAccountKey"].(string), destination.Config["clevertapAccountId"].(string), &clevertapServiceImpl, clevertapService, clevertapServiceImpl.ConnectionConfig)}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        "",
				Destination:     &destination,
			}

			expected := common.AsyncUploadOutput{
				FailedReason:    "got error while transforming the file. failed to open existing file",
				ImportingJobIDs: nil,
				FailedJobIDs:    []int64{1, 2, 3, 4},
				ImportingCount:  0,
				FailedCount:     4,
			}

			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})

		It("TestClevertapErrorWhileUploadingData", func() {
			initClevertap()
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			clevertapService := mocks.NewMockClevertapService(ctrl)
			clevertapServiceImpl := ClevertapSegment.ClevertapServiceImpl{
				BulkApi:   "https://api.clevertap.com/get_custom_list_segment_url",
				NotifyApi: "https://api.clevertap.com/upload_custom_list_segment_completed",
				ConnectionConfig: &ClevertapSegment.ConnectionConfig{
					SourceId:      "source123",
					DestinationId: "destination456",
					Enabled:       true,
					Config: ClevertapSegment.ConnConfig{
						Destination: ClevertapSegment.Destination{
							SchemaVersion: "v1.0",
							SegmentName:   "User Segment A",
							AdminEmail:    "admin@example.com",
							SenderName:    "Rudderstack",
						},
					},
				},
			}

			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: ClevertapSegment.NewClevertapBulkUploader(logger.NOP, stats.NOP, "CLEVERTAP_SEGMENT", destination.Config["clevertapAccountKey"].(string), destination.Config["clevertapAccountId"].(string), &clevertapServiceImpl, clevertapService, clevertapServiceImpl.ConnectionConfig)}

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
			}

			// Mock handling for MakeHTTPRequest
			clevertapService.EXPECT().
				MakeHTTPRequest(gomock.Any()).
				Return([]byte(`{"presignedS3URL": "https://abc.com", "expiry": "2023-12-31T23:59:59Z", "status": "success", "code": 200}`), 200, nil).
				Times(1)

			// Mock expectations
			clevertapService.EXPECT().
				UploadBulkFile(gomock.Any(), "https://abc.com").
				Return(fmt.Errorf("Upload failed with status code: 400")).
				Times(1)

			expected := common.AsyncUploadOutput{
				FailedReason:    "error in uploading the bulk file: Upload failed with status code: 400",
				ImportingJobIDs: nil,
				FailedJobIDs:    []int64{1, 2, 3, 4},
				ImportingCount:  0,
				FailedCount:     4,
			}

			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})

		It("TestClevertapErrorWhilePreSignedURLFetch", func() {
			initClevertap()
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			clevertapService := mocks.NewMockClevertapService(ctrl)
			clevertapServiceImpl := ClevertapSegment.ClevertapServiceImpl{
				BulkApi:   "https://api.clevertap.com/get_custom_list_segment_url",
				NotifyApi: "https://api.clevertap.com/upload_custom_list_segment_completed",
				ConnectionConfig: &ClevertapSegment.ConnectionConfig{
					SourceId:      "source123",
					DestinationId: "destination456",
					Enabled:       true,
					Config: ClevertapSegment.ConnConfig{
						Destination: ClevertapSegment.Destination{
							SchemaVersion: "v1.0",
							SegmentName:   "User Segment A",
							AdminEmail:    "admin@example.com",
							SenderName:    "Rudderstack",
						},
					},
				},
			}

			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: ClevertapSegment.NewClevertapBulkUploader(logger.NOP, stats.NOP, "CLEVERTAP_SEGMENT", destination.Config["clevertapAccountKey"].(string), destination.Config["clevertapAccountId"].(string), &clevertapServiceImpl, clevertapService, clevertapServiceImpl.ConnectionConfig)}

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
			}

			// Mock handling for MakeHTTPRequest
			clevertapService.EXPECT().
				MakeHTTPRequest(gomock.Any()).
				Return([]byte(`{"error": "Invalid Credentials", "status": "fail", "code": 401}`), 401, nil).
				Times(1)

			expected := common.AsyncUploadOutput{
				AbortReason:     "Error while fetching presigned url Error while fetching preSignedUrl: Invalid Credentials",
				ImportingJobIDs: nil,
				AbortJobIDs:     []int64{1, 2, 3, 4},
				ImportingCount:  0,
				AbortCount:      4,
			}

			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})

		It("TestSuccessfulClevertapUpload", func() {
			initClevertap()
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			clevertapService := mocks.NewMockClevertapService(ctrl)
			clevertapServiceImpl := ClevertapSegment.ClevertapServiceImpl{
				BulkApi:   "https://api.clevertap.com/get_custom_list_segment_url",
				NotifyApi: "https://api.clevertap.com/upload_custom_list_segment_completed",
				ConnectionConfig: &ClevertapSegment.ConnectionConfig{
					SourceId:      "source123",
					DestinationId: "destination456",
					Enabled:       true,
					Config: ClevertapSegment.ConnConfig{
						Destination: ClevertapSegment.Destination{
							SchemaVersion: "v1.0",
							SegmentName:   "User Segment A",
							AdminEmail:    "admin@example.com",
							SenderName:    "Rudderstack",
						},
					},
				},
			}
			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: ClevertapSegment.NewClevertapBulkUploader(logger.NOP, stats.NOP, "CLEVERTAP_SEGMENT", destination.Config["clevertapAccountKey"].(string), destination.Config["clevertapAccountId"].(string), &clevertapServiceImpl, clevertapService, clevertapServiceImpl.ConnectionConfig)}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
			}

			// Mock handling for MakeHTTPRequest
			clevertapService.EXPECT().
				MakeHTTPRequest(gomock.Any()).
				Return([]byte(`{"presignedS3URL": "https://abc.com", "expiry": "2023-12-31T23:59:59Z", "status": "success", "code": 200}`), 200, nil).
				Times(1)

			// Mock expectations for UploadBulkFile
			clevertapService.EXPECT().
				UploadBulkFile(gomock.Any(), "https://abc.com").
				Return(nil).
				Times(1)

			clevertapService.EXPECT().
				MakeHTTPRequest(gomock.Any()).
				Return([]byte(`{"Segment ID": 1234, "status": "success", "code": 200}`), 200, nil).
				Times(1)

			expected := common.AsyncUploadOutput{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				ImportingCount:  4,
				FailedCount:     0,
			}

			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})

		It("TestFailureClevertapUploadWhileNaming", func() {
			initClevertap()
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			clevertapService := mocks.NewMockClevertapService(ctrl)
			clevertapServiceImpl := ClevertapSegment.ClevertapServiceImpl{
				BulkApi:   "https://api.clevertap.com/get_custom_list_segment_url",
				NotifyApi: "https://api.clevertap.com/upload_custom_list_segment_completed",
				ConnectionConfig: &ClevertapSegment.ConnectionConfig{
					SourceId:      "source123",
					DestinationId: "destination456",
					Enabled:       true,
					Config: ClevertapSegment.ConnConfig{
						Destination: ClevertapSegment.Destination{
							SchemaVersion: "v1.0",
							SegmentName:   "User Segment A",
							AdminEmail:    "admin@example.com",
							SenderName:    "Rudderstack",
						},
					},
				},
			}
			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: ClevertapSegment.NewClevertapBulkUploader(logger.NOP, stats.NOP, "CLEVERTAP_SEGMENT", destination.Config["clevertapAccountKey"].(string), destination.Config["clevertapAccountId"].(string), &clevertapServiceImpl, clevertapService, clevertapServiceImpl.ConnectionConfig)}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     &destination,
			}

			// Mock handling for MakeHTTPRequest
			clevertapService.EXPECT().
				MakeHTTPRequest(gomock.Any()).
				Return([]byte(`{"presignedS3URL": "https://abc.com", "expiry": "2023-12-31T23:59:59Z", "status": "success", "code": 200}`), 200, nil).
				Times(1)

			// Mock expectations for UploadBulkFile
			clevertapService.EXPECT().
				UploadBulkFile(gomock.Any(), "https://abc.com").
				Return(nil).
				Times(1)

			clevertapService.EXPECT().
				MakeHTTPRequest(gomock.Any()).
				Return([]byte(`{"error": "Email id is either not in the right format or does not belong to a valid admin", "status": "fail", "code": 401}`), 401, nil).
				Times(1)

			expected := common.AsyncUploadOutput{
				AbortJobIDs:  []int64{1, 2, 3, 4},
				FailedJobIDs: nil,
				AbortCount:   4,
				FailedCount:  0,
				AbortReason:  "Error while creating the segment Error while namimng segment: Email id is either not in the right format or does not belong to a valid admin",
			}

			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})
	})
})
