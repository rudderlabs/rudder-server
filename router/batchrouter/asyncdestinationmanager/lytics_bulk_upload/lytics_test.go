package lyticsBulkUpload_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

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
		Config: map[string]any{
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

type lyticsFixture struct {
	ctrl          *gomock.Controller
	lyticsService *mocks.MockLyticsService
}

func newLyticsFixture(t *testing.T) *lyticsFixture {
	t.Helper()
	config.Reset()
	config.Set("BatchRouter.LYTICS_BULK_UPLOAD.MaxUploadLimit", 1*bytesize.MB)
	t.Cleanup(config.Reset)

	tmpDir := t.TempDir()
	t.Setenv("RUDDER_TMPDIR", tmpDir)
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, misc.RudderAsyncDestinationLogs), 0o755))

	initLytics()
	ctrl := gomock.NewController(t)
	return &lyticsFixture{
		ctrl:          ctrl,
		lyticsService: mocks.NewMockLyticsService(ctrl),
	}
}

func TestLyticsBulkUpload(t *testing.T) {
	t.Run("When uploading the file", func(t *testing.T) {
		t.Run("TestLyticsUploadWrongFilepath", func(t *testing.T) {
			f := newLyticsFixture(t)
			LyticsServiceImpl := lyticsBulkUpload.LyticsServiceImpl{
				BulkApi: "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}
			bulkUploader := common.SimpleAsyncDestinationManager{UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader(logger.NOP, stats.NOP, "LYTICS_BULK_UPLOAD", destination.Config["lyticsApiKey"].(string), LyticsServiceImpl.BulkApi, f.lyticsService)}
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
			received := bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})

		t.Run("TestEloquaErrorWhileUploadingData", func(t *testing.T) {
			f := newLyticsFixture(t)

			// Set up the LyticsServiceImpl with the BulkApi endpoint
			LyticsServiceImpl := lyticsBulkUpload.LyticsServiceImpl{
				BulkApi: "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}

			// Set up the SimpleAsyncDestinationManager with the LyticsBulkUploader
			bulkUploader := common.SimpleAsyncDestinationManager{
				UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader(logger.NOP, stats.NOP, "LYTICS_BULK_UPLOAD", destination.Config["lyticsApiKey"].(string), LyticsServiceImpl.BulkApi, f.lyticsService),
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
			f.lyticsService.EXPECT().
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
			received := bulkUploader.Upload(t.Context(), &asyncDestination)

			// Assert that the received output matches the expected output
			require.Equal(t, expected, received)
		})

		t.Run("uploads data successfully", func(t *testing.T) {
			f := newLyticsFixture(t)

			// Set up the LyticsServiceImpl with the BulkApi endpoint
			LyticsServiceImpl := lyticsBulkUpload.LyticsServiceImpl{
				BulkApi: "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp",
			}

			// Set up the SimpleAsyncDestinationManager with the LyticsBulkUploader
			bulkUploader := common.SimpleAsyncDestinationManager{
				UploaderAndTransformer: lyticsBulkUpload.NewLyticsBulkUploader(logger.NOP, stats.NOP, "LYTICS_BULK_UPLOAD", destination.Config["lyticsApiKey"].(string), LyticsServiceImpl.BulkApi, f.lyticsService),
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
			f.lyticsService.EXPECT().
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
			received := bulkUploader.Upload(t.Context(), &asyncDestination)

			// Assert that the received output matches the expected output
			require.Equal(t, expected, received)
		})
	})
}
